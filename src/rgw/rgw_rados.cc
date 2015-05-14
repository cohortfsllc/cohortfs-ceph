// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include <string>
#include <iostream>
#include <vector>
#include <list>
#include <map>
#include <atomic>
#include <errno.h>
#include <stdlib.h>
#include <sys/types.h>

#include "common/ceph_json.h"

#include "common/errno.h"
#include "common/Formatter.h"
#include "common/Throttle.h"

#include "rgw_rados.h"
#include "rgw_cache.h"
#include "rgw_acl.h"
#include "rgw_acl_s3.h" /* for dumping s3policy in debug log */
#include "rgw_metadata.h"
#include "rgw_bucket.h"

#include "cls/rgw/cls_rgw_types.h"
#include "cls/rgw/cls_rgw_client.h"
#include "cls/refcount/cls_refcount_client.h"
#include "cls/version/cls_version_client.h"
#include "cls/log/cls_log_client.h"
#include "cls/statelog/cls_statelog_client.h"
#include "cls/lock/cls_lock_client.h"
#include "cls/user/cls_user_client.h"

#include "rgw_tools.h"

#include "include/ceph_time.h"

#include "auth/Crypto.h" // get_random_bytes()

#include "rgw_log.h"

#include "rgw_gc.h"

#define dout_subsys ceph_subsys_rgw

using namespace std;
using namespace rados;
using namespace std::string_literals;


static string notify_oid_prefix = "notify";
static string dir_oid_prefix = ".dir.";
static string default_storage_vol = ".rgw.buckets";
static string avail_vols = ".vols.avail";

static string zone_info_oid_prefix = "zone_info.";
static string region_info_oid_prefix = "region_info.";

static string default_region_info_oid = "default.region";
static string region_map_oid = "region_map";
static string log_lock_name = "rgw_log_lock";

static RGWObjCategory main_category = RGW_OBJ_CATEGORY_MAIN;

#define RGW_USAGE_OBJ_PREFIX "usage."

#define RGW_DEFAULT_ZONE_ROOT_VOL ".rgw.root"
#define RGW_DEFAULT_REGION_ROOT_VOL ".rgw.root"

#define RGW_STATELOG_OBJ_PREFIX "statelog."


#define dout_subsys ceph_subsys_rgw

void RGWDefaultRegionInfo::dump(Formatter *f) const {
  encode_json("default_region", default_region, f);
}

void RGWDefaultRegionInfo::decode_json(JSONObj *oid) {
  JSONDecoder::decode_json("default_region", default_region, oid);
}

int RGWRegion::get_vol_name(CephContext *cct, string *vol_name)
{
  *vol_name = cct->_conf->rgw_region_root_vol;
  if (vol_name->empty()) {
    *vol_name = RGW_DEFAULT_REGION_ROOT_VOL;
  } else if ((*vol_name)[0] != '.') {
    derr << "ERROR: region root vol name must start with a period" << dendl;
    return -EINVAL;
  }
  return 0;
}

int RGWRegion::read_default(RGWDefaultRegionInfo& default_info)
{
  string vol_name;

  int ret = get_vol_name(cct, &vol_name);
  if (ret < 0) {
    return ret;
  }

  string oid = cct->_conf->rgw_default_region_info_oid;
  if (oid.empty()) {
    oid = default_region_info_oid;
  }

  rgw_bucket vol(vol_name);
  bufferlist bl;
  ret = rgw_get_system_obj(store, NULL, vol, oid, bl, NULL, NULL);
  if (ret < 0)
    return ret;

  try {
    bufferlist::iterator iter = bl.begin();
    ::decode(default_info, iter);
  } catch (buffer::error& err) {
    derr << "error decoding data from " << vol << ":" << oid << dendl;
    return -EIO;
  }

  name = default_info.default_region;

  return 0;
}

int RGWRegion::set_as_default()
{
  string vol_name;
  int ret = get_vol_name(cct, &vol_name);
  if (ret < 0)
    return ret;

  string oid = cct->_conf->rgw_default_region_info_oid;
  if (oid.empty()) {
    oid = default_region_info_oid;
  }

  rgw_bucket vol(vol_name);
  bufferlist bl;

  RGWDefaultRegionInfo default_info;
  default_info.default_region = name;

  ::encode(default_info, bl);

  ret = rgw_put_system_obj(store, vol, oid, bl.c_str(), bl.length(), false, NULL, 0, NULL);
  if (ret < 0)
    return ret;

  return 0;
}

int RGWRegion::init(CephContext *_cct, RGWRados *_store, bool setup_region)
{
  cct = _cct;
  store = _store;

  if (!setup_region)
    return 0;

  string region_name = cct->_conf->rgw_region;

  if (region_name.empty()) {
    RGWDefaultRegionInfo default_info;
    int r = read_default(default_info);
    if (r == -ENOENT) {
      r = create_default();
      if (r == -EEXIST) { /* we may have raced with another region creation,
			     make sure we can read the region info and continue
			     as usual to make sure region creation is complete */
	ldout(cct, 0) << "create_default() returned -EEXIST, we raced with another region creation" << dendl;
	r = read_info(name);
      }
      if (r < 0)
	return r;
      r = set_as_default(); /* set this as default even if we weren't the creators */
      if (r < 0)
	return r;
      /*Re attempt to read region info from newly created default region */
      r = read_default(default_info);
      if (r < 0)
	return r;
    } else if (r < 0) {
      lderr(cct) << "failed reading default region info: " << cpp_strerror(-r) << dendl;
      return r;
    }
    region_name = default_info.default_region;
  }

  return read_info(region_name);
}

int RGWRegion::read_info(const string& region_name)
{
  string vol_name;
  int ret = get_vol_name(cct, &vol_name);
  if (ret < 0)
    return ret;

  rgw_bucket vol(vol_name);
  bufferlist bl;

  name = region_name;

  string oid = region_info_oid_prefix + name;

  ret = rgw_get_system_obj(store, NULL, vol, oid, bl, NULL, NULL);
  if (ret < 0) {
    lderr(cct) << "failed reading region info from " << vol << ":" << oid << ": " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  try {
    bufferlist::iterator iter = bl.begin();
    ::decode(*this, iter);
  } catch (buffer::error& err) {
    ldout(cct, 0) << "ERROR: failed to decode region from " << vol << ":" << oid << dendl;
    return -EIO;
  }

  return 0;
}

int RGWRegion::create_default()
{
  name = "default";
  string zone_name = "default";

  is_master = true;

  RGWRegionPlacementTarget placement_target;
  placement_target.name = "default-placement";
  placement_targets[placement_target.name] = placement_target;
  default_placement = "default-placement";

  RGWZone& default_zone = zones[zone_name];
  default_zone.name = zone_name;

  RGWZoneParams zone_params;
  zone_params.name = zone_name;
  zone_params.init_default(store);

  int r = zone_params.store_info(cct, store, *this);
  if (r < 0) {
    derr << "error storing zone params: " << cpp_strerror(-r) << dendl;
    return r;
  }

  r = store_info(true);
  if (r < 0) {
    derr << "error storing region info: " << cpp_strerror(-r) << dendl;
    return r;
  }

  return 0;
}

int RGWRegion::store_info(bool exclusive)
{
  string vol_name;
  int ret = get_vol_name(cct, &vol_name);
  if (ret < 0)
    return ret;

  rgw_bucket vol(vol_name);

  string oid = region_info_oid_prefix + name;

  bufferlist bl;
  ::encode(*this, bl);
  ret = rgw_put_system_obj(store, vol, oid, bl.c_str(), bl.length(), exclusive, NULL, 0, NULL);

  return ret;
}

int RGWRegion::equals(const string& other_region)
{
  if (is_master && other_region.empty())
    return true;

  return (name == other_region);
}

void RGWZoneParams::init_default(RGWRados *store)
{
  domain_root = ".rgw"s;
  control_vol = ".rgw.control"s;
  gc_vol = ".rgw.gc"s;
  log_vol = ".log"s;
  intent_log_vol = ".intent-log"s;
  usage_log_vol = ".usage"s;
  user_keys_vol = ".users"s;
  user_email_vol = ".users.email"s;
  user_swift_vol = ".users.swift"s;
  user_uid_vol = ".users.uid"s;

  /* check for old vols config */
  rgw_obj oid(domain_root, avail_vols);
  int r =  store->obj_stat(NULL, oid, NULL, NULL, NULL, NULL, NULL);
  if (r < 0) {
    ldout(store->ctx(), 0) << "couldn't find old data placement vols config,"
      " setting up new ones for the zone" << dendl;
    /* a new system, let's set new placement info */
    RGWZonePlacementInfo default_placement;
    default_placement.index_vol = ".rgw.buckets.index";
    default_placement.data_vol = ".rgw.buckets";
    placement_vols["default-placement"] = default_placement;
  }
}

int RGWZoneParams::get_vol_name(CephContext *cct, string *vol_name)
{
  *vol_name = cct->_conf->rgw_zone_root_vol;
  if (vol_name->empty()) {
    *vol_name = RGW_DEFAULT_ZONE_ROOT_VOL;
  } else if ((*vol_name)[0] != '.') {
    derr << "ERROR: zone root vol name must start with a period" << dendl;
    return -EINVAL;
  }

  return 0;
}

void RGWZoneParams::init_name(CephContext *cct, RGWRegion& region)
{
  name = cct->_conf->rgw_zone;

  if (name.empty()) {
    name = region.master_zone;

    if (name.empty()) {
      name = "default";
    }
  }
}

int RGWZoneParams::init(CephContext *cct, RGWRados *store, RGWRegion& region)
{
  init_name(cct, region);

  string vol_name;
  int ret = get_vol_name(cct, &vol_name);
  if (ret < 0)
    return ret;

  rgw_bucket vol(vol_name);
  bufferlist bl;

  string oid = zone_info_oid_prefix + name;
  ret = rgw_get_system_obj(store, NULL, vol, oid, bl, NULL, NULL);
  if (ret < 0)
    return ret;

  try {
    bufferlist::iterator iter = bl.begin();
    ::decode(*this, iter);
  } catch (buffer::error& err) {
    ldout(cct, 0) << "ERROR: failed to decode zone info from " << vol << ":" << oid << dendl;
    return -EIO;
  }

  is_master = (name == region.master_zone) || (region.master_zone.empty() && name == "default");

  ldout(cct, 2) << "zone " << name << " is " << (is_master ? "" : "NOT ") << "master" << dendl;

  return 0;
}

int RGWZoneParams::store_info(CephContext *cct, RGWRados *store, RGWRegion& region)
{
  init_name(cct, region);

  string vol_name;
  int ret = get_vol_name(cct, &vol_name);
  if (ret < 0)
    return ret;

  rgw_bucket vol(vol_name);
  string oid = zone_info_oid_prefix + name;

  bufferlist bl;
  ::encode(*this, bl);
  ret = rgw_put_system_obj(store, vol, oid, bl.c_str(), bl.length(), false, NULL, 0, NULL);

  return ret;
}

void RGWRegionMap::encode(bufferlist& bl) const {
  ENCODE_START(3, 1, bl);
  ::encode(regions, bl);
  ::encode(master_region, bl);
  ::encode(bucket_quota, bl);
  ::encode(user_quota, bl);
  ENCODE_FINISH(bl);
}

void RGWRegionMap::decode(bufferlist::iterator& bl) {
  DECODE_START(3, bl);
  ::decode(regions, bl);
  ::decode(master_region, bl);

  if (struct_v >= 2)
    ::decode(bucket_quota, bl);
  if (struct_v >= 3)
    ::decode(user_quota, bl);
  DECODE_FINISH(bl);

  regions_by_api.clear();
  for (map<string, RGWRegion>::iterator iter = regions.begin();
       iter != regions.end(); ++iter) {
    RGWRegion& region = iter->second;
    regions_by_api[region.api_name] = region;
    if (region.is_master) {
      master_region = region.name;
    }
  }
}

void RGWRegionMap::get_params(string& vol_name, string& oid)
{
  vol_name = cct->_conf->rgw_zone_root_vol;
  if (vol_name.empty()) {
    vol_name = RGW_DEFAULT_ZONE_ROOT_VOL;
  }
  oid = region_map_oid;
}

int RGWRegionMap::read(RGWRados *store)
{
  string vol_name, oid;

  get_params(vol_name, oid);

  rgw_bucket vol(vol_name);

  bufferlist bl;
  int ret = rgw_get_system_obj(store, NULL, vol, oid, bl, NULL, NULL);
  if (ret < 0)
    return ret;


  lock_guard l(lock);
  try {
    bufferlist::iterator iter = bl.begin();
    ::decode(*this, iter);
  } catch (buffer::error& err) {
    ldout(cct, 0) << "ERROR: failed to decode region map info from " << vol << ":" << oid << dendl;
    return -EIO;
  }

  return 0;
}

int RGWRegionMap::store(RGWRados *store)
{
  string vol_name, oid;

  get_params(vol_name, oid);

  rgw_bucket vol(vol_name);

  lock_guard l(lock);

  bufferlist bl;
  ::encode(*this, bl);
  int ret = rgw_put_system_obj(store, vol, oid, bl.c_str(), bl.length(), false, NULL, 0, NULL);

  return ret;
}

int RGWRegionMap::update(RGWRegion& region)
{
  lock_guard l(lock);

  if (region.is_master && !region.equals(master_region)) {
    derr << "cannot update region map, master_region conflict" << dendl;
    return -EINVAL;
  }
  map<string, RGWRegion>::iterator iter = regions.find(region.name);
  if (iter != regions.end()) {
    RGWRegion& old_region = iter->second;
    if (!old_region.api_name.empty()) {
      regions_by_api.erase(old_region.api_name);
    }
  }
  regions[region.name] = region;

  if (!region.api_name.empty()) {
    regions_by_api[region.api_name] = region;
  }

  if (region.is_master) {
    master_region = region.name;
  }
  return 0;
}


void RGWObjVersionTracker::prepare_op_for_read(ObjOpUse op)
{
  obj_version *check_objv = version_for_check();

  if (check_objv) {
    cls_version_check(op, *check_objv, VER_COND_EQ);
  }

  cls_version_read(op, &read_version);
}

void RGWObjVersionTracker::prepare_op_for_write(ObjOpUse op)
{
  obj_version *check_objv = version_for_check();
  obj_version *modify_version = version_for_write();

  if (check_objv) {
    cls_version_check(op, *check_objv, VER_COND_EQ);
  }

  if (modify_version) {
    cls_version_set(op, *modify_version);
  } else {
    cls_version_inc(op);
  }
}

void RGWObjManifest::obj_iterator::update_explicit_pos()
{
  ofs = explicit_iter->first;
  stripe_ofs = ofs;

  map<uint64_t, RGWObjManifestPart>::iterator next_iter = explicit_iter;
  ++next_iter;
  if (next_iter != manifest->objs.end()) {
    stripe_size = next_iter->first - ofs;
  } else {
    stripe_size = manifest->obj_size - ofs;
  }
}

void RGWObjManifest::obj_iterator::seek(uint64_t o)
{
  ofs = o;
  if (manifest->explicit_objs) {
    explicit_iter = manifest->objs.upper_bound(ofs);
    if (explicit_iter != manifest->objs.begin()) {
      --explicit_iter;
    }
    if (ofs >= manifest->obj_size) {
      ofs = manifest->obj_size;
      return;
    }
    update_explicit_pos();
    update_location();
    return;
  }
  if (o < manifest->get_head_size()) {
    rule_iter = manifest->rules.begin();
    stripe_ofs = 0;
    stripe_size = manifest->get_head_size();
    cur_part_id = rule_iter->second.start_part_num;
    update_location();
    return;
  }

  rule_iter = manifest->rules.upper_bound(ofs);
  next_rule_iter = rule_iter;
  if (rule_iter != manifest->rules.begin()) {
    --rule_iter;
  }

  RGWObjManifestRule& rule = rule_iter->second;

  if (rule.part_size > 0) {
    cur_part_id = rule.start_part_num + (ofs - rule.start_ofs) / rule.part_size;
  } else {
    cur_part_id = rule.start_part_num;
  }
  part_ofs = rule.start_ofs + (cur_part_id - rule.start_part_num) * rule.part_size;

  if (rule.stripe_max_size > 0) {
    cur_stripe = (ofs - part_ofs) / rule.stripe_max_size;

    stripe_ofs = part_ofs + cur_stripe * rule.stripe_max_size;
    if (!cur_part_id && manifest->get_head_size() > 0) {
      cur_stripe++;
    }
  } else {
    cur_stripe = 0;
    stripe_ofs = part_ofs;
  }

  if (!rule.part_size) {
    stripe_size = rule.stripe_max_size;
    stripe_size = MIN(manifest->get_obj_size() - stripe_ofs, stripe_size);
  } else {
    stripe_size = rule.part_size - (ofs - stripe_ofs);
    stripe_size = MIN(stripe_size, rule.stripe_max_size);
  }

  update_location();
}

void RGWObjManifest::obj_iterator::update_location()
{
  if (manifest->explicit_objs) {
    location = explicit_iter->second.loc;
    return;
  }

  const rgw_obj& head = manifest->get_head();

  if (ofs < manifest->get_head_size()) {
    location = head;
    return;
  }

  manifest->get_implicit_location(cur_part_id, cur_stripe, ofs, &location);
}

void RGWObjManifest::obj_iterator::operator++()
{
  if (manifest->explicit_objs) {
    ++explicit_iter;

    if (explicit_iter == manifest->objs.end()) {
      ofs = manifest->obj_size;
      return;
    }

    update_explicit_pos();

    update_location();
    return;
  }

  uint64_t obj_size = manifest->get_obj_size();
  uint64_t head_size = manifest->get_head_size();

  if (ofs == obj_size) {
    return;
  }

  if (manifest->rules.empty()) {
    return;
  }

  /* are we still pointing at the head? */
  if (ofs < head_size) {
    rule_iter = manifest->rules.begin();
    RGWObjManifestRule *rule = &rule_iter->second;
    ofs = MIN(head_size, obj_size);
    stripe_ofs = ofs;
    cur_stripe = 1;
    stripe_size = MIN(obj_size - ofs, rule->stripe_max_size);
    if (rule->part_size > 0) {
      stripe_size = MIN(stripe_size, rule->part_size);
    }
    update_location();
    return;
  }

  RGWObjManifestRule *rule = &rule_iter->second;

  stripe_ofs += rule->stripe_max_size;
  cur_stripe++;

  if (rule->part_size > 0) {
    /* multi part, multi stripes object */

    if (stripe_ofs >= part_ofs + rule->part_size) {
      /* moved to the next part */
      cur_stripe = 0;
      part_ofs += rule->part_size;
      stripe_ofs = part_ofs;

      bool last_rule = (next_rule_iter == manifest->rules.end());
      /* move to the next rule? */
      if (!last_rule && stripe_ofs >= next_rule_iter->second.start_ofs) {
	rule_iter = next_rule_iter;
	last_rule = (next_rule_iter == manifest->rules.end());
	if (!last_rule) {
	  ++next_rule_iter;
	}
	cur_part_id = rule_iter->second.start_part_num;
      } else {
	cur_part_id++;
      }

      rule = &rule_iter->second;
    }

    stripe_size = MIN(rule->part_size - (stripe_ofs - part_ofs), rule->stripe_max_size);
  }

  ofs = stripe_ofs;
  if (ofs > obj_size) {
    ofs = obj_size;
    stripe_ofs = ofs;
    stripe_size = 0;
  }

  update_location();
}

int RGWObjManifest::generator::create_begin(CephContext *cct, RGWObjManifest *_m, rgw_bucket& _b, rgw_obj& _h)
{
  manifest = _m;

  bucket = _b;
  manifest->set_tail_bucket(_b);
  manifest->set_head(_h);
  last_ofs = 0;

  char buf[33];
  gen_rand_alphanumeric(cct, buf, sizeof(buf) - 1);

  if (manifest->get_prefix().empty()) {
    string oid_prefix = ".";
    oid_prefix.append(buf);
    oid_prefix.append("_");

    manifest->set_prefix(oid_prefix);
  }

  bool found = manifest->get_rule(0, &rule);
  if (!found) {
    derr << "ERROR: manifest->get_rule() could not find rule" << dendl;
    return -EIO;
  }

  uint64_t head_size = manifest->get_head_size();

  if (head_size > 0) {
    cur_stripe_size = head_size;
  } else {
    cur_stripe_size = rule.stripe_max_size;
  }

  cur_part_id = rule.start_part_num;

  manifest->get_implicit_location(cur_part_id, cur_stripe, 0, &cur_obj);

  manifest->update_iterators();

  return 0;
}

int RGWObjManifest::generator::create_next(uint64_t ofs)
{
  if (ofs < last_ofs) /* only going forward */
    return -EINVAL;

  string obj_name = manifest->prefix;

  uint64_t max_head_size = manifest->get_max_head_size();

  if (ofs <= max_head_size) {
    manifest->set_head_size(ofs);
  }

  if (ofs >= max_head_size) {
    manifest->set_head_size(max_head_size);
    cur_stripe = (ofs - max_head_size) / rule.stripe_max_size;
    cur_stripe_size =  rule.stripe_max_size;

    if (cur_part_id == 0 && max_head_size > 0) {
      cur_stripe++;
    }
  }

  last_ofs = ofs;
  manifest->set_obj_size(ofs);


  manifest->get_implicit_location(cur_part_id, cur_stripe, ofs, &cur_obj);

  manifest->update_iterators();

  return 0;
}

const RGWObjManifest::obj_iterator& RGWObjManifest::obj_begin()
{
  return begin_iter;
}

const RGWObjManifest::obj_iterator& RGWObjManifest::obj_end()
{
  return end_iter;
}

void RGWObjManifest::get_implicit_location(uint64_t cur_part_id,
					   uint64_t cur_stripe, uint64_t ofs,
					   rgw_obj *location)
{
  string oid = prefix;

  if (!cur_part_id) {
    if (ofs < max_head_size) {
      *location = head_obj;
      return;
    } else {
      char buf[16];
      snprintf(buf, sizeof(buf), "%d", (int)cur_stripe);
      oid += buf;
    }
  } else {
    char buf[32];
    if (cur_stripe == 0) {
      snprintf(buf, sizeof(buf), ".%d", (int)cur_part_id);
      oid += buf;
    } else {
      snprintf(buf, sizeof(buf), ".%d_%d", (int)cur_part_id, (int)cur_stripe);
      oid += buf;
    }
  }

  rgw_bucket *bucket;

  if (!tail_bucket.name.empty()) {
    bucket = &tail_bucket;
  } else {
    bucket = &head_obj.bucket;
  }
}

RGWObjManifest::obj_iterator RGWObjManifest::obj_find(uint64_t ofs)
{
  if (ofs > obj_size) {
    ofs = obj_size;
  }
  RGWObjManifest::obj_iterator iter(this);
  iter.seek(ofs);
  return iter;
}

int RGWObjManifest::append(RGWObjManifest& m)
{
  if (explicit_objs || m.explicit_objs) {
    return append_explicit(m);
  }

  if (rules.empty()) {
    *this = m;
    return 0;
  }

  if (prefix.empty()) {
    prefix = m.prefix;
  } else if (prefix != m.prefix) {
    return append_explicit(m);
  }

  map<uint64_t, RGWObjManifestRule>::iterator miter = m.rules.begin();
  if (miter == m.rules.end()) {
    return append_explicit(m);
  }

  for (; miter != m.rules.end(); ++miter) {
    map<uint64_t, RGWObjManifestRule>::reverse_iterator last_rule = rules.rbegin();

    RGWObjManifestRule& rule = last_rule->second;

    if (rule.part_size == 0) {
      rule.part_size = obj_size - rule.start_ofs;
    }

    RGWObjManifestRule& next_rule = miter->second;
    if (!next_rule.part_size) {
      next_rule.part_size = m.obj_size - next_rule.start_ofs;
    }

    if (rule.part_size != next_rule.part_size ||
	rule.stripe_max_size != next_rule.stripe_max_size) {
      append_rules(m, miter);
      break;
    }

    uint64_t expected_part_num = rule.start_part_num + 1;
    if (rule.part_size > 0) {
      expected_part_num = rule.start_part_num + (obj_size + next_rule.start_ofs - rule.start_ofs) / rule.part_size;
    }

    if (expected_part_num != next_rule.start_part_num) {
      append_rules(m, miter);
      break;
    }
  }

  set_obj_size(obj_size + m.obj_size);

  return 0;
}

void RGWObjManifest::append_rules(RGWObjManifest& m, map<uint64_t, RGWObjManifestRule>::iterator& miter)
{
  for (; miter != m.rules.end(); ++miter) {
    RGWObjManifestRule rule = miter->second;
    rule.start_ofs += obj_size;
    rules[rule.start_ofs] = rule;
  }
}

void RGWObjManifest::convert_to_explicit()
{
  if (explicit_objs) {
    return;
  }
  obj_iterator iter = obj_begin();

  while (iter != obj_end()) {
    RGWObjManifestPart& part = objs[iter.get_stripe_ofs()];
    part.loc = iter.get_location();
    part.loc_ofs = 0;

    uint64_t ofs = iter.get_stripe_ofs();
    ++iter;
    uint64_t next_ofs = iter.get_stripe_ofs();

    part.size = next_ofs - ofs;
  }

  explicit_objs = true;
  rules.clear();
  prefix.clear();
}

int RGWObjManifest::append_explicit(RGWObjManifest& m)
{
  if (!explicit_objs) {
    convert_to_explicit();
  }
  if (!m.explicit_objs) {
    m.convert_to_explicit();
  }
  map<uint64_t, RGWObjManifestPart>::iterator iter;
  uint64_t base = obj_size;
  for (iter = m.objs.begin(); iter != m.objs.end(); ++iter) {
    RGWObjManifestPart& part = iter->second;
    objs[base + iter->first] = part;
  }
  obj_size += m.obj_size;

  return 0;
}

bool RGWObjManifest::get_rule(uint64_t ofs, RGWObjManifestRule *rule)
{
  if (rules.empty()) {
    return false;
  }

  map<uint64_t, RGWObjManifestRule>::iterator iter = rules.upper_bound(ofs);
  if (iter != rules.begin()) {
    --iter;
  }

  *rule = iter->second;

  return true;
}

void RGWObjVersionTracker::generate_new_write_ver(CephContext *cct)
{
  write_version.ver = 1;
#define TAG_LEN 24

  write_version.tag.clear();
  append_rand_alpha(cct, write_version.tag, write_version.tag, TAG_LEN);
}

int RGWPutObjProcessor::complete(string& etag, time_t *mtime, time_t set_mtime, map<string, bufferlist>& attrs)
{
  int r = do_complete(etag, mtime, set_mtime, attrs);
  if (r < 0)
    return r;

  is_complete = true;
  return 0;
}

RGWPutObjProcessor::~RGWPutObjProcessor()
{
  if (is_complete)
    return;

  list<rgw_obj>::iterator iter;
  for (iter = objs.begin(); iter != objs.end(); ++iter) {
    rgw_obj& oid = *iter;
    int r = store->delete_obj(obj_ctx, bucket_owner, oid);
    if (r < 0 && r != -ENOENT) {
      ldout(store->ctx(), 0) << "WARNING: failed to remove oid ("
			     << oid << "), leaked" << dendl;
    }
  }
}

int RGWPutObjProcessor_Plain::prepare(RGWRados *store, void *obj_ctx)
{
  RGWPutObjProcessor::prepare(store, obj_ctx);

  oid.init(bucket, obj_str);

  return 0;
};

int RGWPutObjProcessor_Plain::handle_data(bufferlist& bl, off_t _ofs,
					  const wait_ref& cb)
{
  if (ofs != _ofs)
    return -EINVAL;

  data.append(bl);
  ofs += bl.length();

  return 0;
}

int RGWPutObjProcessor_Plain::do_complete(string& etag, time_t *mtime,
					  time_t set_mtime,
					  map<string, bufferlist>& attrs)
{
  RGWRados::PutObjMetaExtraParams params;
  params.set_mtime = set_mtime;
  params.mtime = mtime;
  params.data = &data;
  params.owner = bucket_owner;

  int r = store->put_obj_meta(obj_ctx, oid, data.length(), attrs,
			      RGW_OBJ_CATEGORY_MAIN, PUT_OBJ_CREATE,
			      params);
  return r;
}


int RGWPutObjProcessor_Aio::handle_obj_data(rgw_obj& oid, bufferlist& bl,
					    off_t ofs, off_t abs_ofs,
					    const wait_ref& cb)
{
  if ((uint64_t)abs_ofs + bl.length() > obj_len)
    obj_len = abs_ofs + bl.length();

  // For the first call pass -1 as the offset to
  // do a write_full.
  int r = store->aio_put_obj_data(NULL, oid,
				  bl, ((ofs != 0) ? ofs : -1),
				  false, cb);

  return r;
}

wait_ref RGWPutObjProcessor_Aio::pop_pending()
{
  wait_ref w(std::move(pending.front()));
  pending.pop_front();
  return w;
}

int RGWPutObjProcessor_Aio::wait_pending_front()
{
  wait_ref cb(pop_pending());
  return cb.get()->wait();
}

bool RGWPutObjProcessor_Aio::pending_has_completed()
{
  if (pending.empty())
    return false;

  return pending.front()->complete();
}

int RGWPutObjProcessor_Aio::drain_pending()
{
  int ret = 0;
  while (!pending.empty()) {
    int r = wait_pending_front();
    if (r < 0)
      ret = r;
  }
  return ret;
}

int RGWPutObjProcessor_Aio::throttle_data(wait_ref& cb)
{
  pending.emplace_back(std::move(cb));
  size_t orig_size = pending.size();
  while (pending_has_completed()) {
    int r = wait_pending_front();
    if (r < 0)
      return r;
  }

  /* resize window in case messages are draining too fast */
  if (orig_size - pending.size() >= max_chunks) {
    max_chunks++;
  }

  if (pending.size() > max_chunks) {
    int r = wait_pending_front();
    if (r < 0)
      return r;
  }
  return 0;
}

int RGWPutObjProcessor_Atomic::write_data(bufferlist& bl, off_t ofs,
					  const wait_ref& cb)
{
  if (ofs >= next_part_ofs) {
    int r = prepare_next_part(ofs);
    if (r < 0) {
      return r;
    }
  }

  return RGWPutObjProcessor_Aio::handle_obj_data(
    cur_obj, bl, ofs - cur_part_ofs, ofs, cb);
}

int RGWPutObjProcessor_Atomic::handle_data(bufferlist& bl, off_t ofs,
					   const wait_ref& cb)
{
  if (extra_data_len) {
    size_t extra_len = bl.length();
    if (extra_len > extra_data_len)
      extra_len = extra_data_len;

    bufferlist extra;
    bl.splice(0, extra_len, &extra);
    extra_data_bl.append(extra);

    extra_data_len -= extra_len;
    if (bl.length() == 0) {
      return 0;
    }
  }

  uint64_t max_chunk_size = store->get_max_chunk_size();

  pending_data_bl.claim_append(bl);
  if (pending_data_bl.length() < max_chunk_size)
    return 0;

  pending_data_bl.splice(0, max_chunk_size, &bl);

  if (!data_ofs && !immutable_head()) {
    first_chunk.claim(bl);
    obj_len = (uint64_t)first_chunk.length();
    int r = prepare_next_part(first_chunk.length());
    if (r < 0) {
      return r;
    }
    data_ofs = obj_len;
    return 0;
  }
  off_t write_ofs = data_ofs;
  data_ofs = write_ofs + bl.length();
  return write_data(bl, write_ofs, cb);
}

int RGWPutObjProcessor_Atomic::prepare(RGWRados *store, void *obj_ctx)
{
  RGWPutObjProcessor::prepare(store, obj_ctx);

  head_obj.init(bucket, obj_str);

  uint64_t max_chunk_size = store->get_max_chunk_size();

  manifest.set_trivial_rule(max_chunk_size, store->ctx()->_conf->rgw_obj_stripe_size);

  int r = manifest_gen.create_begin(store->ctx(), &manifest, bucket, head_obj);
  if (r < 0) {
    return r;
  }

  return 0;
}

int RGWPutObjProcessor_Atomic::prepare_next_part(off_t ofs) {

  int ret = manifest_gen.create_next(ofs);
  if (ret < 0) {
    lderr(store->ctx()) << "ERROR: manifest_gen.create_next() returned ret=" << ret << dendl;
    return ret;
  }
  cur_part_ofs = ofs;
  next_part_ofs = ofs + manifest_gen.cur_stripe_max_size();
  cur_obj = manifest_gen.get_cur_obj();
  add_obj(cur_obj);

  return 0;
};

int RGWPutObjProcessor_Atomic::complete_parts()
{
  if (obj_len > (uint64_t)cur_part_ofs) {
    return prepare_next_part(obj_len);
  }
  return 0;
}

int RGWPutObjProcessor_Atomic::complete_writing_data()
{
  if (!data_ofs && !immutable_head()) {
    first_chunk.claim(pending_data_bl);
    obj_len = (uint64_t)first_chunk.length();
  }
  if (pending_data_bl.length()) {
    wait_ref cb(std::make_unique<rados::CB_Waiter>());
    int r = write_data(pending_data_bl, data_ofs, cb);
    if (r < 0) {
      ldout(store->ctx(), 0) << "ERROR: write_data() returned " << r << dendl;
      return r;
    }
    r = throttle_data(cb);
    if (r < 0) {
      ldout(store->ctx(), 0) << "ERROR: throttle_data() returned " << r
			     << dendl;
      return r;
    }
  }
  int r = complete_parts();
  if (r < 0) {
    return r;
  }

  r = drain_pending();
  if (r < 0)
    return r;

  return 0;
}

int RGWPutObjProcessor_Atomic::do_complete(string& etag, time_t *mtime, time_t set_mtime, map<string, bufferlist>& attrs) {
  int r = complete_writing_data();
  if (r < 0)
    return r;

  store->set_atomic(obj_ctx, head_obj);

  RGWRados::PutObjMetaExtraParams extra_params;

  extra_params.data = &first_chunk;
  extra_params.manifest = &manifest;
  extra_params.ptag = &unique_tag; /* use req_id as operation tag */
  extra_params.mtime = mtime;
  extra_params.set_mtime = set_mtime;
  extra_params.owner = bucket_owner;

  r = store->put_obj_meta(obj_ctx, head_obj, obj_len, attrs,
			  RGW_OBJ_CATEGORY_MAIN, PUT_OBJ_CREATE,
			  extra_params);
  return r;
}

RGWObjState *RGWRadosCtx::get_state(rgw_obj& oid) {
  if (oid.object.size()) {
    return &objs_state[oid];
  } else {
    rgw_obj new_obj(store->zone.domain_root, oid.bucket.name);
    return &objs_state[new_obj];
  }
}

void RGWRadosCtx::set_atomic(rgw_obj& oid) {
  if (oid.object.size()) {
    objs_state[oid].is_atomic = true;
  } else {
    rgw_obj new_obj(store->zone.domain_root, oid.bucket.name);
    objs_state[new_obj].is_atomic = true;
  }
}

void RGWRadosCtx::set_prefetch_data(rgw_obj& oid) {
  if (oid.object.size()) {
    objs_state[oid].prefetch_data = true;
  } else {
    rgw_obj new_obj(store->zone.domain_root, oid.bucket.name);
    objs_state[new_obj].prefetch_data = true;
  }
}

void RGWRados::finalize()
{
  delete meta_mgr;
  delete data_log;
  if (use_gc_thread) {
    gc->stop_processor();
    delete gc;
    gc = NULL;
  }
  delete rest_master_conn;

  map<string, RGWRESTConn *>::iterator iter;
  for (iter = zone_conn_map.begin(); iter != zone_conn_map.end(); ++iter) {
    RGWRESTConn *conn = iter->second;
    delete conn;
  }

  for (iter = region_conn_map.begin(); iter != region_conn_map.end(); ++iter) {
    RGWRESTConn *conn = iter->second;
    delete conn;
  }
  RGWQuotaHandler::free_handler(quota_handler);
}

/**
 * Initialize the RADOS instance and prepare to do other ops
 * Returns 0 on success, -ERR# on failure.
 */
int RGWRados::init_rados()
{
  int ret;

  max_chunk_size = cct->_conf->rgw_max_chunk_size;

  ret = rc.connect();
  if (ret < 0)
   return ret;

  meta_mgr = new RGWMetadataManager(cct, this);
  data_log = new RGWDataChangesLog(cct, this);

  return ret;
}

/**
 * Initialize the RADOS instance and prepare to do other ops
 * Returns 0 on success, -ERR# on failure.
 */
int RGWRados::init_complete()
{
  int ret;

  ret = region.init(cct, this);
  if (ret < 0)
    return ret;

  ret = zone.init(cct, this, region);
  if (ret < 0)
    return ret;

  ret = region_map.read(this);
  if (ret < 0) {
    if (ret != -ENOENT) {
      ldout(cct, 0) << "WARNING: cannot read region map" << dendl;
    }
    ret = region_map.update(region);
    if (ret < 0) {
      ldout(cct, 0) << "ERROR: failed to update regionmap with local region info" << dendl;
      return -EIO;
    }
  } else {
    string master_region = region_map.master_region;
    if (master_region.empty()) {
      lderr(cct) << "ERROR: region map does not specify master region" << dendl;
      return -EINVAL;
    }
    map<string, RGWRegion>::iterator iter = region_map.regions.find(master_region);
    if (iter == region_map.regions.end()) {
      lderr(cct) << "ERROR: bad region map: inconsistent master region" << dendl;
      return -EINVAL;
    }
    RGWRegion& region = iter->second;
    rest_master_conn = new RGWRESTConn(cct, this, region.endpoints);

    for (iter = region_map.regions.begin(); iter != region_map.regions.end(); ++iter) {
      RGWRegion& region = iter->second;

      region_conn_map[region.name] = new RGWRESTConn(cct, this, region.endpoints);
    }
  }

  map<string, RGWZone>::iterator ziter;
  for (ziter = region.zones.begin(); ziter != region.zones.end(); ++ziter) {
    const string& name = ziter->first;
    RGWZone& z = ziter->second;
    if (name != zone.name) {
      ldout(cct, 20) << "generating connection object for zone " << name << dendl;
      zone_conn_map[name] = new RGWRESTConn(cct, this, z.endpoints);
    } else {
      zone_public_config = z;
    }
  }

  ret = open_root_vol_ctx();
  if (ret < 0)
    return ret;

  ret = open_gc_vol_ctx();
  if (ret < 0)
    return ret;

  vols_initialized = true;

  gc = new RGWGC();
  gc->initialize(cct, this);

  if (use_gc_thread)
    gc->start_processor();

  quota_handler = RGWQuotaHandler::generate_handler(this, quota_threads);

  return ret;
}

/**
 * Initialize the RADOS instance and prepare to do other ops
 * Returns 0 on success, -ERR# on failure.
 */
int RGWRados::initialize()
{
  int ret;

  ret = init_rados();
  if (ret < 0)
    return ret;

  ret = init_complete();

  return ret;
}

int RGWRados::list_raw_prefixed_objs(string vol_name, const string& prefix, list<string>& result)
{
  rgw_bucket vol(vol_name);
  bool is_truncated;
  RGWListRawObjsCtx ctx;
  do {
    list<string> oids;
    int r = list_raw_objects(vol, prefix, 1000,
			     ctx, oids, &is_truncated);
    if (r < 0) {
      return r;
    }
    list<string>::iterator iter;
    for (iter = oids.begin(); iter != oids.end(); ++iter) {
      string& val = *iter;
      if (val.size() > prefix.size())
	result.push_back(val.substr(prefix.size()));
    }
  } while (is_truncated);

  return 0;
}

int RGWRados::list_regions(list<string>& regions)
{
  string vol_name;
  int ret = RGWRegion::get_vol_name(cct, &vol_name);
  if (ret < 0)
    return ret;

  return list_raw_prefixed_objs(vol_name, region_info_oid_prefix, regions);
}

int RGWRados::list_zones(list<string>& zones)
{
  string vol_name;
  int ret = RGWZoneParams::get_vol_name(cct, &vol_name);
  if (ret < 0)
    return ret;

  return list_raw_prefixed_objs(vol_name, zone_info_oid_prefix, zones);
}

/**
 * Open the vol used as root for this gateway
 * Returns: 0 on success, -ERR# otherwise.
 */
int RGWRados::open_root_vol_ctx()
{
  const string& vol_name = zone.domain_root.name;
  root_vol = rc.lookup_volume(vol_name);
  if (!root_vol) {
    int r = rc.objecter->create_volume(vol_name);
    if (r == -EEXIST)
      r = 0;
    if (r < 0)
      return r;

    root_vol = rc.lookup_volume(vol_name);
  }

  return 0;
}

int RGWRados::open_gc_vol_ctx()
{
  gc_vol = rc.lookup_volume(zone.gc_vol.name);
  if (!gc_vol) {
    int r = rc.objecter->create_volume(zone.gc_vol.name);
    if (r == -EEXIST)
      r = 0;
    if (r < 0)
      return r;

    gc_vol = rc.lookup_volume(zone.gc_vol.name);
  }

  return 0;
}

int RGWRados::open_bucket_vol(const string& bucket_name,
			      const string& vol_name, VolumeRef& vol)
{
  vol = rc.lookup_volume(vol_name);

  if (!vol && !vols_initialized)
    return -ENOENT;

  int r = rc.objecter->create_volume(vol_name);
  if (r < 0 && r != -EEXIST)
    return r;

  vol = rc.lookup_volume(vol_name);
  if (!vol)
    return -ENOENT;

  return 0;
}

int RGWRados::open_bucket_data_vol(rgw_bucket& bucket, VolumeRef& data)
{
  int r = open_bucket_vol(bucket.name, bucket.data_vol, data);
  if (r < 0)
    return r;

  return 0;
}

int RGWRados::open_bucket_data_extra_vol(rgw_bucket& bucket,
					 VolumeRef& data_ctx)
{
  int r = open_bucket_vol(bucket.name, bucket.data_extra_vol, data_ctx);
  if (r < 0)
    return r;

  return 0;
}

int RGWRados::open_bucket_index_vol(rgw_bucket& bucket,
				    VolumeRef& index_ctx)
{
  int r = open_bucket_vol(bucket.name, bucket.index_vol, index_ctx);
  if (r < 0)
    return r;

  return 0;
}

/**
 * set up a bucket listing.
 * handle is filled in.
 * Returns 0 on success, -ERR# otherwise.
 */
int RGWRados::list_buckets_init(RGWAccessHandle *handle)
{
    //librados::ObjectIterator *state = new librados::ObjectIterator(root_vol_ctx.objects_begin());
    //*handle = (RGWAccessHandle)state;
  return 0;
}

/**
 * get the next bucket in the listing.
 * oid is filled in,
 * handle is updated.
 * returns 0 on success, -ERR# otherwise.
 */
int RGWRados::list_buckets_next(RGWObjEnt& oid, RGWAccessHandle *handle)
{
//  librados::ObjectIterator *state = (librados::ObjectIterator *)*handle;

//  do {
//    if (*state == root_vol_ctx.objects_end()) {
//	delete state;
//	return -ENOENT;
//    }

//    oid.name = (*state)->first;
//    (*state)++;
//  } while (oid.name[0] == '.'); /* skip all entries starting with '.' */

  return 0;
}


/**** logs ****/

struct log_list_state {
  string prefix;
  VolumeRef vol;
};

int RGWRados::log_list_init(const string& prefix, RGWAccessHandle *handle)
{
  log_list_state *state = new log_list_state;
  string& log_vol = zone.log_vol.name;
  state->vol = rc.lookup_volume(log_vol);
  if (!state->vol) {
    delete state;
    return -ENOENT;
  }
  state->prefix = prefix;
  *handle = (RGWAccessHandle)state;
  return 0;
}

int RGWRados::log_list_next(RGWAccessHandle handle, string *name)
{
  log_list_state *state = static_cast<log_list_state *>(handle);
  while (true) {
      delete state;
      return -ENOENT;
    }
  return 0;
}

int RGWRados::log_remove(const string& name)
{
  VolumeRef vol = rc.lookup_volume(zone.log_vol.name);
  if (!vol)
    return -ENOENT;
  return rc.objecter->remove(name, vol);
}

struct log_show_state {
  VolumeRef vol;
  bufferlist bl;
  bufferlist::iterator p;
  string name;
  uint64_t pos;
  bool eof;
  log_show_state() : pos(0), eof(false) {}
};

int RGWRados::log_show_init(const string& name, RGWAccessHandle *handle)
{
  log_show_state *state = new log_show_state;
  VolumeRef vol = rc.lookup_volume(zone.log_vol.name);
  if (!vol) {
    delete state;
    return -ENOENT;
  }
  state->name = name;
  *handle = (RGWAccessHandle)state;
  return 0;
}

int RGWRados::log_show_next(RGWAccessHandle handle, rgw_log_entry *entry)
{
  log_show_state *state = static_cast<log_show_state *>(handle);
  off_t off = state->p.get_off();

  ldout(cct, 10) << "log_show_next pos " << state->pos << " bl " << state->bl.length()
	   << " off " << off
	   << " eof " << (int)state->eof
	   << dendl;
  // read some?
  unsigned chunk = 1024*1024;
  if ((state->bl.length() - off) < chunk/2 && !state->eof) {
    bufferlist more;
    int r = rc.objecter->read(state->name, state->vol, state->pos, chunk,
			      &more);
    if (r < 0)
      return r;
    state->pos += r;
    bufferlist old;
    try {
      old.substr_of(state->bl, off, state->bl.length() - off);
    } catch (buffer::error& err) {
      return -EINVAL;
    }
    state->bl.clear();
    state->bl.claim(old);
    state->bl.claim_append(more);
    state->p = state->bl.begin();
    if ((unsigned)r < chunk)
      state->eof = true;
    ldout(cct, 10) << " read " << r << dendl;
  }

  if (state->p.end())
    return 0;  // end of file
  try {
    ::decode(*entry, state->p);
  }
  catch (const buffer::error &e) {
    return -EINVAL;
  }
  return 1;
}

/**
 * usage_log_hash: get usage log key hash, based on name and index
 *
 * Get the usage object name. Since a user may have more than 1
 * object holding that info (multiple shards), we use index to
 * specify that shard number. Once index exceeds max shards it
 * wraps.
 * If name is not being set, results for all users will be returned
 * and index will wrap only after total shards number.
 *
 * @param cct [in] ceph context
 * @param name [in] user name
 * @param hash [out] hash value
 * @param index [in] shard index number
 */
static void usage_log_hash(CephContext *cct, const string& name, string& hash,
			   uint32_t index)
{
  uint32_t val = index;

  if (!name.empty()) {
    int max_user_shards = max(cct->_conf->rgw_usage_max_user_shards, 1);
    val %= max_user_shards;
    val += ceph_str_hash_linux(name.c_str(), name.size());
  }
  char buf[16];
  int max_shards = max(cct->_conf->rgw_usage_max_shards, 1);
  snprintf(buf, sizeof(buf), RGW_USAGE_OBJ_PREFIX "%u", (unsigned)(val % max_shards));
  hash = buf;
}

int RGWRados::log_usage(map<rgw_user_bucket, RGWUsageBatch>& usage_info)
{
  uint32_t index = 0;

  map<string, rgw_usage_log_info> log_objs;

  string hash;
  string last_user;

  /* restructure usage map, zone by object hash */
  map<rgw_user_bucket, RGWUsageBatch>::iterator iter;
  for (iter = usage_info.begin(); iter != usage_info.end(); ++iter) {
    const rgw_user_bucket& ub = iter->first;
    RGWUsageBatch& info = iter->second;

    if (ub.user.empty()) {
      ldout(cct, 0) << "WARNING: RGWRados::log_usage(): user name empty (bucket=" << ub.bucket << "), skipping" << dendl;
      continue;
    }

    if (ub.user != last_user) {
      /* index *should* be random, but why waste extra cycles
	 in most cases max user shards is not going to exceed 1,
	 so just incrementing it */
      usage_log_hash(cct, ub.user, hash, index++);
    }
    last_user = ub.user;
    vector<rgw_usage_log_entry>& v = log_objs[hash].entries;

    map<ceph::real_time, rgw_usage_log_entry>::iterator miter;
    for (miter = info.m.begin(); miter != info.m.end(); ++miter) {
      v.push_back(miter->second);
    }
  }

  map<string, rgw_usage_log_info>::iterator liter;

  for (liter = log_objs.begin(); liter != log_objs.end(); ++liter) {
    int r = cls_obj_usage_log_add(liter->first, liter->second);
    if (r < 0)
      return r;
  }
  return 0;
}

int RGWRados::read_usage(string& user, uint32_t max_entries,
			 bool *is_truncated, RGWUsageIter& usage_iter,
			 map<rgw_user_bucket, rgw_usage_log_entry>& usage)
{
  uint32_t num = max_entries;
  string hash, first_hash;
  usage_log_hash(cct, user, first_hash, 0);

  if (usage_iter.index) {
    usage_log_hash(cct, user, hash, usage_iter.index);
  } else {
    hash = first_hash;
  }

  usage.clear();

  do {
    map<rgw_user_bucket, rgw_usage_log_entry> ret_usage;
    map<rgw_user_bucket, rgw_usage_log_entry>::iterator iter;

    int ret =  cls_obj_usage_log_read(hash, user, num,
				      usage_iter.read_iter, ret_usage,
				      is_truncated);
    if (ret == -ENOENT)
      goto next;

    if (ret < 0)
      return ret;

    num -= ret_usage.size();

    for (iter = ret_usage.begin(); iter != ret_usage.end(); ++iter) {
      usage[iter->first].aggregate(iter->second);
    }

next:
    if (!*is_truncated) {
      usage_iter.read_iter.clear();
      usage_log_hash(cct, user, hash, ++usage_iter.index);
    }
  } while (num && !*is_truncated && hash != first_hash);
  return 0;
}

int RGWRados::trim_usage(string& user)
{
  uint32_t index = 0;
  string hash, first_hash;
  usage_log_hash(cct, user, first_hash, index);

  hash = first_hash;

  do {
    int ret =  cls_obj_usage_log_trim(hash, user);
    if (ret == -ENOENT)
      goto next;

    if (ret < 0)
      return ret;

next:
    usage_log_hash(cct, user, hash, ++index);
  } while (hash != first_hash);

  return 0;
}

void RGWRados::shard_name(const string& prefix, unsigned max_shards,
			  const string& key, string& name)
{
  uint32_t val = ceph_str_hash_linux(key.c_str(), key.size());
  char buf[16];
  snprintf(buf, sizeof(buf), "%u", (unsigned)(val % max_shards));
  name = prefix + buf;
}

void RGWRados::shard_name(const string& prefix, unsigned max_shards,
			  const string& section, const string& key, string& name)
{
  uint32_t val = ceph_str_hash_linux(key.c_str(), key.size());
  val ^= ceph_str_hash_linux(section.c_str(), section.size());
  char buf[16];
  snprintf(buf, sizeof(buf), "%u", (unsigned)(val % max_shards));
  name = prefix + buf;
}

void RGWRados::time_log_prepare_entry(cls_log_entry& entry,
				      const ceph::real_time& ut,
				      string& section, string& key,
				      bufferlist& bl)
{
  cls_log_add_prepare_entry(entry, ut, section, key, bl);
}

int RGWRados::time_log_add(const string& oid, const ceph::real_time& ut,
			   const string& section, const string& key,
			   bufferlist& bl)
{
  VolumeRef vol;

  vol = rc.lookup_volume(zone.log_vol.name);
  if (!vol) {
    rgw_bucket volb(zone.log_vol.name);
    int r = create_vol(volb);
    if (r < 0)
      return r;

    // retry
    vol = rc.lookup_volume(zone.log_vol.name);
  }
  if (!vol)
    return -EIO;

  ObjectOperation op(vol->op());
  cls_log_add(op, ut, section, key, bl);

  return rc.objecter->mutate(oid, vol, op);
}

int RGWRados::time_log_add(const string& oid, list<cls_log_entry>& entries)
{
  string& log_vol = zone.log_vol.name;
  VolumeRef vol = rc.lookup_volume(log_vol);
  if (!vol) {
    rgw_bucket volb(log_vol);
    int r = create_vol(volb);
    if (r < 0)
      return r;

    // retry
    vol = rc.lookup_volume(log_vol);
  }
  if (!vol)
    return -EIO;

  ObjectOperation op(vol->op());
  cls_log_add(op, entries);

  return rc.objecter->mutate(oid, vol, op);
}

int RGWRados::time_log_list(const string& oid, ceph::real_time& start_time,
			    ceph::real_time& end_time,
			    int max_entries, list<cls_log_entry>& entries,
			    const string& marker,
			    string *out_marker,
			    bool *truncated)
{
  const string& log_vol = zone.log_vol.name;
  VolumeRef vol(rc.lookup_volume(log_vol));
  if (!vol)
    return -ENOENT;

  ObjectOperation op(vol->op());

  cls_log_list(op, start_time, end_time, marker, max_entries, entries,
	       out_marker, truncated);

  return rc.objecter->read(oid, vol, op);
}

int RGWRados::time_log_info(const string& oid, cls_log_header *header)
{

  const string& log_vol = zone.log_vol.name;
  VolumeRef vol(rc.lookup_volume(log_vol));
  if (!vol)
    return -ENOENT;

  ObjectOperation op(vol->op());

  cls_log_info(op, header);

  return rc.objecter->read(oid, vol, op);
}

int RGWRados::time_log_trim(const string& oid,
			    const ceph::real_time& start_time,
			    const ceph::real_time& end_time,
			    const string& from_marker, const string& to_marker)
{
  const string& log_vol = zone.log_vol.name;
  VolumeRef vol(rc.lookup_volume(log_vol));
  if (!vol)
    return -ENOENT;

  return cls_log_trim(rc.objecter, vol, oid, start_time, end_time,
		      from_marker, to_marker);
}


int RGWRados::lock_exclusive(rgw_bucket& bucket, const string& oid,
			     ceph::timespan& duration,
			     string& zone_id, string& owner_id)
{
  const string& log_vol = zone.log_vol.name;
  VolumeRef vol(rc.lookup_volume(log_vol));
  if (!vol)
    return -ENOENT;

  rados::cls::lock::Lock l(log_lock_name);
  l.set_duration(duration);
  l.set_cookie(owner_id);
  l.set_tag(zone_id);
  l.set_renew(true);

  return l.lock_exclusive(rc.objecter, vol, oid);
}

int RGWRados::unlock(rgw_bucket& bucket, const string& oid, string& zone_id,
		     string& owner_id)
{
  const string& log_vol = zone.log_vol.name;
  VolumeRef vol(rc.lookup_volume(log_vol));
  if (!vol)
    return -ENOENT;

  rados::cls::lock::Lock l(log_lock_name);
  l.set_tag(zone_id);
  l.set_cookie(owner_id);

  return l.unlock(rc.objecter, vol, oid);
}

int RGWRados::decode_policy(bufferlist& bl, ACLOwner *owner)
{
  bufferlist::iterator i = bl.begin();
  RGWAccessControlPolicy policy(cct);
  try {
    policy.decode_owner(i);
  } catch (buffer::error& err) {
    ldout(cct, 0) << "ERROR: could not decode policy, caught buffer::error" << dendl;
    return -EIO;
  }
  *owner = policy.get_owner();
  return 0;
}

int rgw_policy_from_attrset(CephContext *cct, map<string, bufferlist>& attrset, RGWAccessControlPolicy *policy)
{
  map<string, bufferlist>::iterator aiter = attrset.find(RGW_ATTR_ACL);
  if (aiter == attrset.end())
    return -EIO;

  bufferlist& bl = aiter->second;
  bufferlist::iterator iter = bl.begin();
  try {
    policy->decode(iter);
  } catch (buffer::error& err) {
    ldout(cct, 0) << "ERROR: could not decode policy, caught buffer::error" << dendl;
    return -EIO;
  }
  if (cct->_conf->subsys.should_gather(ceph_subsys_rgw, 15)) {
    RGWAccessControlPolicy_S3 *s3policy = static_cast<RGWAccessControlPolicy_S3 *>(policy);
    ldout(cct, 15) << "Read AccessControlPolicy";
    s3policy->to_xml(*_dout);
    *_dout << dendl;
  }
  return 0;
}

/**
 * get listing of the objects in a bucket.
 * bucket: bucket to list contents of
 * max: maximum number of results to return
 * prefix: only return results that match this prefix
 * delim: do not include results that match this string.
 *     Any skipped results will have the matching portion of their name
 *     inserted in common_prefixes with a "true" mark.
 * marker: if filled in, begin the listing with this object.
 * result: the objects are put in here.
 * common_prefixes: if delim is filled in, any matching prefixes are placed
 *     here.
 */
int RGWRados::list_objects(rgw_bucket& bucket, int max, string& prefix, string& delim,
			   string& marker, vector<RGWObjEnt>& result,
			   map<string, bool>& common_prefixes,
			   bool get_content_type,
			   bool *is_truncated, RGWAccessListFilter *filter)
{
  int count = 0;
  bool truncated;

  if (bucket_is_system(bucket)) {
    return -EINVAL;
  }
  result.clear();

  rgw_obj marker_obj, prefix_obj;
  marker_obj.set_obj(marker);
  string cur_marker = marker_obj.object;

  prefix_obj.set_obj(prefix);
  string cur_prefix = prefix_obj.object;

  do {
    std::map<string, RGWObjEnt> ent_map;
    int r = cls_bucket_list(bucket, cur_marker, cur_prefix, max - count, ent_map,
			    &truncated, &cur_marker);
    if (r < 0)
      return r;

    std::map<string, RGWObjEnt>::iterator eiter;
    for (eiter = ent_map.begin(); eiter != ent_map.end(); ++eiter) {
      string oid = eiter->first;
      string key = oid;

      if (filter && !filter->filter(oid, key))
	continue;

      if (prefix.size() &&  ((oid).compare(0, prefix.size(), prefix) != 0))
	continue;

      if (!delim.empty()) {
	int delim_pos = oid.find(delim, prefix.size());

	if (delim_pos >= 0) {
	  common_prefixes[oid.substr(0, delim_pos + 1)] = true;
	  continue;
	}
      }

      RGWObjEnt ent = eiter->second;
      ent.name = oid;
      result.push_back(ent);
      count++;
    }
  } while (truncated && count < max);

  if (is_truncated)
    *is_truncated = truncated;

  return 0;
}

/**
 * create a rados vol, associated meta info
 * returns 0 on success, -ERR# otherwise.
 */
int RGWRados::create_vol(rgw_bucket& bucket)
{
  int ret = 0;

  string vol = bucket.index_vol;

  ret = rc.vol_create(vol);
  if (ret == -EEXIST)
    ret = 0;
  if (ret < 0)
    return ret;

  if (bucket.data_vol != vol) {
    ret = rc.vol_create(bucket.data_vol);
    if (ret == -EEXIST)
      ret = 0;
    if (ret < 0)
      return ret;
  }

  return 0;
}

int RGWRados::init_bucket_index(rgw_bucket& bucket)
{
  VolumeRef index_vol;
  int r = open_bucket_index_vol(bucket, index_vol);
  if (r < 0)
    return r;

  string dir_oid =  dir_oid_prefix;
  dir_oid.append(bucket.marker);

  ObjectOperation op(index_vol->op());
  op->create(true);
  r = cls_rgw_init_index(rc, index_vol, op, dir_oid);
  if (r < 0 && r != -EEXIST)
    return r;

  return 0;
}

/**
 * create a bucket with name bucket and the given list of attrs
 * returns 0 on success, -ERR# otherwise.
 */
int RGWRados::create_bucket(RGWUserInfo& owner, rgw_bucket& bucket,
			    const string& region_name,
			    const string& placement_rule,
			    map<std::string, bufferlist>& attrs,
			    RGWBucketInfo& info,
			    obj_version *pobjv,
			    obj_version *pep_objv,
			    time_t creation_time,
			    rgw_bucket *pmaster_bucket,
			    bool exclusive)
{
#define MAX_CREATE_RETRIES 20 /* need to bound retries */
  string selected_placement_rule;
  for (int i = 0; i < MAX_CREATE_RETRIES; i++) {
    int ret = 0;
    ret = select_bucket_placement(owner, region_name, placement_rule,
				  bucket.name, bucket,
				  &selected_placement_rule);
    if (ret < 0)
      return ret;
    bufferlist bl;
    uint32_t nop = 0;
    ::encode(nop, bl);

    if (!pmaster_bucket) {
      uint64_t iid = instance_id();
      uint64_t bid = next_bucket_id();
      char buf[32];
      snprintf(buf, sizeof(buf), "%s.%llu.%llu", zone.name.c_str(), (long long)iid, (long long)bid);
      bucket.marker = buf;
      bucket.bucket_id = bucket.marker;
    } else {
      bucket.marker = pmaster_bucket->marker;
      bucket.bucket_id = pmaster_bucket->bucket_id;
    }

    string dir_oid =  dir_oid_prefix;
    dir_oid.append(bucket.marker);

    int r = init_bucket_index(bucket);
    if (r < 0)
      return r;

    RGWObjVersionTracker& objv_tracker = info.objv_tracker;

    if (pobjv) {
      objv_tracker.write_version = *pobjv;
    } else {
      objv_tracker.generate_new_write_ver(cct);
    }

    info.bucket = bucket;
    info.owner = owner.user_id;
    info.region = region_name;
    info.placement_rule = selected_placement_rule;
    if (!creation_time)
      time(&info.creation_time);
    else
      info.creation_time = creation_time;
    ret = put_linked_bucket_info(info, exclusive, 0, pep_objv, &attrs, true);
    if (ret == -EEXIST) {
       /* we need to reread the info and return it, caller will have a use for it */
      info.objv_tracker.clear();
      r = get_bucket_info(NULL, bucket.name, info, NULL, NULL);
      if (r < 0) {
	if (r == -ENOENT) {
	  continue;
	}
	ldout(cct, 0) << "get_bucket_info returned " << r << dendl;
	return r;
      }

      /* only remove it if it's a different bucket instance */
      if (info.bucket.bucket_id != bucket.bucket_id) {
	/* remove bucket meta instance */
	string entry;
	get_bucket_instance_entry(bucket, entry);
	r = rgw_bucket_instance_remove_entry(this, entry, &info.objv_tracker);
	if (r < 0)
	  return r;

	/* remove bucket index */
	VolumeRef index_vol; // context for new bucket
	int r = open_bucket_index_vol(bucket, index_vol);
	if (r < 0)
	  return r;

	rc.objecter->remove(dir_oid, index_vol);
      }
      /* ret == -ENOENT here */
    }
    return ret;
  }

  /* this is highly unlikely */
  ldout(cct, 0) << "ERROR: could not create bucket, continuously raced with bucket creation and removal" << dendl;
  return -ENOENT;
}

int RGWRados::select_new_bucket_location(RGWUserInfo& user_info, const string& region_name, const string& request_rule,
					 const string& bucket_name, rgw_bucket& bucket, string *pselected_rule)
{
  /* first check that rule exists within the specific region */
  map<string, RGWRegion>::iterator riter = region_map.regions.find(region_name);
  if (riter == region_map.regions.end()) {
    ldout(cct, 0) << "could not find region " << region_name << " in region map" << dendl;
    return -EINVAL;
  }
  /* now check that tag exists within region */
  RGWRegion& region = riter->second;

  /* find placement rule. Hierarchy: request rule > user default rule > region default rule */
  string rule = request_rule;
  if (rule.empty()) {
    rule = user_info.default_placement;
    if (rule.empty())
      rule = region.default_placement;
  }

  if (rule.empty()) {
    ldout(cct, 0) << "misconfiguration, should not have an empty placement rule name" << dendl;
    return -EIO;
  }

  if (!rule.empty()) {
    map<string, RGWRegionPlacementTarget>::iterator titer = region.placement_targets.find(rule);
    if (titer == region.placement_targets.end()) {
      ldout(cct, 0) << "could not find placement rule " << rule << " within region " << dendl;
      return -EINVAL;
    }

    /* now check tag for the rule, whether user is permitted to use rule */
    RGWRegionPlacementTarget& target_rule = titer->second;
    if (!target_rule.user_permitted(user_info.placement_tags)) {
      ldout(cct, 0) << "user not permitted to use placement rule" << dendl;
      return -EPERM;
    }
  }

  if (pselected_rule)
    *pselected_rule = rule;

  return set_bucket_location_by_rule(rule, bucket_name, bucket);
}

int RGWRados::set_bucket_location_by_rule(const string& location_rule, const std::string& bucket_name, rgw_bucket& bucket)
{
  bucket.name = bucket_name;

  if (location_rule.empty()) {
    /* we can only reach here if we're trying to set a bucket location from a bucket
     * created on a different zone, using a legacy / default vol configuration
     */
    return select_legacy_bucket_placement(bucket_name, bucket);
  }

  /*
   * make sure that zone has this rule configured. We're
   * checking it for the local zone, because that's where this bucket object is going to
   * reside.
   */
  map<string, RGWZonePlacementInfo>::iterator piter = zone.placement_vols.find(location_rule);
  if (piter == zone.placement_vols.end()) {
    /* couldn't find, means we cannot really place data for this bucket in this zone */
    if (region.equals(region_name)) {
      /* that's a configuration error, zone should have that rule, as we're within the requested
       * region */
      return -EINVAL;
    } else {
      /* oh, well, data is not going to be placed here, bucket object is just a placeholder */
      return 0;
    }
  }

  RGWZonePlacementInfo& placement_info = piter->second;

  bucket.data_vol = placement_info.data_vol;
  bucket.data_extra_vol = placement_info.data_extra_vol;
  bucket.index_vol = placement_info.index_vol;

  return 0;

}

int RGWRados::select_bucket_placement(RGWUserInfo& user_info, const string& region_name, const string& placement_rule,
				      const string& bucket_name, rgw_bucket& bucket, string *pselected_rule)
{
  if (!zone.placement_vols.empty()) {
    return select_new_bucket_location(user_info, region_name, placement_rule, bucket_name, bucket, pselected_rule);
  }

  if (pselected_rule)
    pselected_rule->clear();

  return select_legacy_bucket_placement(bucket_name, bucket);
}

int RGWRados::select_legacy_bucket_placement(const string& bucket_name, rgw_bucket& bucket)
{
  bufferlist map_bl;
  map<string, bufferlist> m;
  string vol_name;
  bool write_map = false;

  rgw_obj oid(zone.domain_root, avail_vols);

  int ret = rgw_get_system_obj(this, NULL, zone.domain_root, avail_vols, map_bl, NULL, NULL);
  if (ret < 0) {
    goto read_omap;
  }

  try {
    bufferlist::iterator iter = map_bl.begin();
    ::decode(m, iter);
  } catch (buffer::error& err) {
    ldout(cct, 0) << "ERROR: couldn't decode avail_vols" << dendl;
  }

read_omap:
  if (m.empty()) {
    bufferlist header;
    ret = omap_get_all(oid, header, m);

    write_map = true;
  }

  if (ret < 0 || m.empty()) {
    vector<string> names;
    names.push_back(default_storage_vol);
    vector<int> retcodes;
    bufferlist bl;
    ret = create_vols(names, retcodes);
    if (ret < 0)
      return ret;
    ret = omap_set(oid, default_storage_vol, bl);
    if (ret < 0)
      return ret;
    m[default_storage_vol] = bl;
  }

  if (write_map) {
    bufferlist new_bl;
    ::encode(m, new_bl);
    ret = put_obj_data(NULL, oid, new_bl.c_str(), -1, new_bl.length(), false);
    if (ret < 0) {
      ldout(cct, 0) << "WARNING: could not save avail vols map info ret=" << ret << dendl;
    }
  }

  map<string, bufferlist>::iterator miter;
  if (m.size() > 1) {
    vector<string> v;
    for (miter = m.begin(); miter != m.end(); ++miter) {
      v.push_back(miter->first);
    }

    uint32_t r;
    ret = get_random_bytes((char *)&r, sizeof(r));
    if (ret < 0)
      return ret;

    int i = r % v.size();
    vol_name = v[i];
  } else {
    miter = m.begin();
    vol_name = miter->first;
  }
  bucket.data_vol = vol_name;
  bucket.index_vol = vol_name;
  bucket.name = bucket_name;

  return 0;

}

int RGWRados::update_placement_map()
{
  bufferlist header;
  map<string, bufferlist> m;
  rgw_obj oid(zone.domain_root, avail_vols);
  int ret = omap_get_all(oid, header, m);
  if (ret < 0)
    return ret;

  bufferlist new_bl;
  ::encode(m, new_bl);
  ret = put_obj_data(NULL, oid, new_bl.c_str(), -1, new_bl.length(), false);
  if (ret < 0) {
    ldout(cct, 0) << "WARNING: could not save avail vols map info ret=" << ret << dendl;
  }

  return ret;
}

int RGWRados::add_bucket_placement(std::string& new_vol)
{
  auto v = rc.lookup_volume(new_vol);
  if (!v) // DNE, or something
    return -ENOENT;

  rgw_obj oid(zone.domain_root, avail_vols);
  bufferlist empty_bl;
  int ret = omap_set(oid, new_vol, empty_bl);

  // don't care about return value
  update_placement_map();

  return ret;
}

int RGWRados::remove_bucket_placement(std::string& old_vol)
{
  rgw_obj oid(zone.domain_root, avail_vols);
  int ret = omap_del(oid, old_vol);

  // don't care about return value
  update_placement_map();

  return ret;
}

int RGWRados::list_placement_set(std::set<string>& names)
{
  bufferlist header;
  map<string, bufferlist> m;

  rgw_obj oid(zone.domain_root, avail_vols);
  int ret = omap_get_all(oid, header, m);
  if (ret < 0)
    return ret;

  names.clear();
  map<string, bufferlist>::iterator miter;
  for (miter = m.begin(); miter != m.end(); ++miter) {
    names.insert(miter->first);
  }

  return names.size();
}

int RGWRados::create_vols(vector<string>& names, vector<int>& retcodes)
{
  vector<string>::iterator iter;

  for (iter = names.begin(); iter != names.end(); ++iter) {
    string& name = *iter;
    int r = rc.vol_create(name);
    if (r < 0) {
      ldout(cct, 0) << "WARNING: volume_create returned " << r << dendl;
    }
  }

  return 0;
}


int RGWRados::get_obj_vol(const rgw_obj& obj, VolumeRef& vol)
{
  rgw_bucket bucket;
  string oid, key;
  get_obj_bucket_and_oid(obj, bucket, oid);

  int r;

  if (!obj.is_in_extra_data()) {
    r = open_bucket_data_vol(bucket, vol);
  } else {
    r = open_bucket_data_extra_vol(bucket, vol);
  }
  if (r < 0)
    return r;

  return 0;
}

int RGWRados::get_obj_ref(const rgw_obj& oid, rgw_rados_ref *ref, rgw_bucket *bucket, bool ref_system_obj)
{
  get_obj_bucket_and_oid(oid, *bucket, ref->oid);

  int r;

  if (ref_system_obj && ref->oid.empty()) {
    ref->oid = bucket->name;
    *bucket = zone.domain_root;

    r = open_bucket_data_vol(*bucket, ref->vol);
  } else if (!oid.is_in_extra_data()) {
    r = open_bucket_data_vol(*bucket, ref->vol);
  } else {
    r = open_bucket_data_extra_vol(*bucket, ref->vol);
  }
  if (r < 0)
    return r;

  return 0;
}

/**
 * Write/overwrite an object to the bucket storage.
 * bucket: the bucket to store the object in
 * oid: the object name
 * data: the object contents/value
 * size: the amount of data to write (data must be this long)
 * mtime: if non-NULL, writes the given mtime to the bucket storage
 * attrs: all the given attrs are written to bucket storage for the given object
 * exclusive: create object exclusively
 * Returns: 0 on success, -ERR# otherwise.
 */
int RGWRados::put_obj_meta_impl(void *ctx, rgw_obj& oid,  uint64_t size,
				time_t *mtime, map<string, bufferlist>& attrs,
				RGWObjCategory category, int flags,
				map<string, bufferlist>* rmattrs,
				const bufferlist *data,
				RGWObjManifest *manifest,
				const string *ptag,
				list<string> *remove_objs,
				bool modify_version,
				RGWObjVersionTracker *objv_tracker,
				time_t set_mtime,
				const string& bucket_owner)
{
  rgw_bucket bucket;
  rgw_rados_ref ref;
  int r = get_obj_ref(oid, &ref, &bucket);
  if (r < 0)
    return r;

  RGWRadosCtx *rctx = static_cast<RGWRadosCtx *>(ctx);

  ObjectOperation op(ref.vol->op());

  RGWObjState *state = NULL;

  if (flags & PUT_OBJ_EXCL) {
    if (!(flags & PUT_OBJ_CREATE))
	return -EINVAL;
    op->create(true); // exclusive create
  } else {
    bool reset_obj = (flags & PUT_OBJ_CREATE) != 0;
    r = prepare_atomic_for_write(rctx, oid, op, &state, reset_obj, ptag);
    if (r < 0)
      return r;
  }

  if (objv_tracker) {
    objv_tracker->prepare_op_for_write(op);
  }

  if (data) {
    /* if we want to overwrite the data, we also want to overwrite the
       xattrs, so just remove the object */
    op->write_full(*data);
  }

  string etag;
  string content_type;
  bufferlist acl_bl;

  map<string, bufferlist>::iterator iter;
  if (rmattrs) {
    for (iter = rmattrs->begin(); iter != rmattrs->end(); ++iter) {
      const string& name = iter->first;
      op->rmxattr(name);
    }
  }

  if (manifest) {
    /* remove existing manifest attr */
    iter = attrs.find(RGW_ATTR_MANIFEST);
    if (iter != attrs.end())
      attrs.erase(iter);

    bufferlist bl;
    ::encode(*manifest, bl);
    op->setxattr(RGW_ATTR_MANIFEST, bl);
  }

  for (iter = attrs.begin(); iter != attrs.end(); ++iter) {
    const string& name = iter->first;
    bufferlist& bl = iter->second;

    if (!bl.length())
      continue;

    op->setxattr(name, bl);

    if (name.compare(RGW_ATTR_ETAG) == 0) {
      etag = bl.c_str();
    } else if (name.compare(RGW_ATTR_CONTENT_TYPE) == 0) {
      content_type = bl.c_str();
    } else if (name.compare(RGW_ATTR_ACL) == 0) {
      acl_bl = bl;
    }
  }

  if (!op->size())
    return 0;

  string index_tag;

  if (state) {
    index_tag = state->write_tag;
  }

  r = prepare_update_index(NULL, bucket, CLS_RGW_OP_ADD, oid, index_tag);
  if (r < 0)
    return r;

  const auto& volid = ref.vol->id;

  r = rc.objecter->mutate(ref.oid, ref.vol, op);
  if (r < 0) /* we can expect to get -ECANCELED if object was replaced under,
		or -ENOENT if was removed, or -EEXIST if it did not exist
		before and now it does */
    goto done_cancel;

  if (objv_tracker) {
    objv_tracker->apply_write();
  }

  r = complete_atomic_overwrite(rctx, state, oid);
  if (r < 0) {
    ldout(cct, 0) << "ERROR: complete_atomic_overwrite returned r=" << r << dendl;
  }

  r = complete_update_index(bucket, oid.object, index_tag, volid, size,
			    ceph::real_clock::now(), etag, content_type,
			    &acl_bl, category, remove_objs);
  if (r < 0)
    goto done_cancel;

  if (mtime) {
    *mtime = set_mtime;
  }

  if (state) {
    /* update quota cache */
    quota_handler->update_stats(bucket_owner, bucket, (state->exists ? 0 : 1), size, state->size);
  }

  return 0;

done_cancel:
  int ret = complete_update_index_cancel(bucket, oid.object, index_tag);
  if (ret < 0) {
    ldout(cct, 0) << "ERROR: complete_update_index_cancel() returned ret=" << ret << dendl;
  }
  /* we lost in a race. There are a few options:
   * - existing object was rewritten (ECANCELED)
   * - non existing object was created (EEXIST)
   * - object was removed (ENOENT)
   * should treat it as a success
   */
  if ((r == -ECANCELED || r == -ENOENT) ||
      (!(flags & PUT_OBJ_EXCL) && r == -EEXIST)) {
    r = 0;
  }

  return r;
}

/**
 * Write/overwrite an object to the bucket storage.
 * bucket: the bucket to store the object in
 * oid: the object name
 * data: the object contents/value
 * offset: the offet to write to in the object
 *	   If this is -1, we will overwrite the whole object.
 * size: the amount of data to write (data must be this long)
 * attrs: all the given attrs are written to bucket storage for the given object
 * Returns: 0 on success, -ERR# otherwise.
 */
int RGWRados::put_obj_data(void *ctx, rgw_obj& oid, const char *data,
			   off_t ofs, size_t len, bool exclusive)
{
  bufferlist bl;
  wait_ref w(std::make_unique<rados::CB_Waiter>());
  bl.append(data, len);
  int r = aio_put_obj_data(ctx, oid, bl, ofs, exclusive, w);
  if (r < 0)
    return r;
  return w->wait();
}

int RGWRados::aio_put_obj_data(void *ctx, rgw_obj& oid, bufferlist& bl,
			       off_t ofs, bool exclusive,
			       const wait_ref& cb)
{
  rgw_rados_ref ref;
  rgw_bucket bucket;
  int r = get_obj_ref(oid, &ref, &bucket);
  if (r < 0) {
    return r;
  }

  ObjectOperation op(ref.vol->op());

  if (exclusive)
    op->create(true);

  if (ofs == -1) {
    op->write_full(bl);
  } else {
    op->write(ofs, bl);
  }
  r = rc.objecter->mutate(ref.oid, ref.vol, op, ceph::real_clock::now(),
			  nullptr, *cb);
  if (r < 0)
    return r;

  return 0;
}

class RGWRadosPutObj : public RGWGetDataCB
{
  rgw_obj oid;
  RGWPutObjProcessor_Atomic *processor;
  RGWOpStateSingleOp *opstate;
  void (*progress_cb)(off_t, void *);
  void *progress_data;
public:
  RGWRadosPutObj(RGWPutObjProcessor_Atomic *p, RGWOpStateSingleOp *_ops,
		 void (*_progress_cb)(off_t, void *), void *_progress_data)
    : RGWGetDataCB(_ops->cct), processor(p), opstate(_ops),
      progress_cb(_progress_cb), progress_data(_progress_data) {}
  int handle_data(bufferlist& bl, off_t ofs, off_t len,
		  wait_ref& cb) {
    progress_cb(ofs, progress_data);

    int ret = processor->handle_data(bl, ofs, cb);
    if (ret < 0)
      return ret;

    if (opstate) {
      /* need to update opstate repository with new state. This is ratelimited, so we're not
       * really doing it every time
       */
      ret = opstate->renew_state();
      if (ret < 0) {
	/* could not renew state! might have been marked as cancelled */
	return ret;
      }
    }

    ret = processor->throttle_data(cb);
    if (ret < 0)
      return ret;

    return 0;
  }

  void set_extra_data_len(uint64_t len) {
    RGWGetDataCB::set_extra_data_len(len);
    processor->set_extra_data_len(len);
  }

  int complete(string& etag, time_t *mtime, time_t set_mtime, map<string, bufferlist>& attrs) {
    return processor->complete(etag, mtime, set_mtime, attrs);
  }
};

/*
 * prepare attrset, either replace it with new attrs, or keep it (other than acls).
 */
static void set_copy_attrs(map<string, bufferlist>& src_attrs, map<string,
			   bufferlist>& attrs, bool replace_attrs,
			   bool intra_region)
{
  if (replace_attrs) {
    if (!attrs[RGW_ATTR_ETAG].length())
      attrs[RGW_ATTR_ETAG] = src_attrs[RGW_ATTR_ETAG];

    src_attrs = attrs;
  } else {
    /* copying attrs from source, however acls should only be copied if it's intra-region operation */
    if (!intra_region)
      src_attrs[RGW_ATTR_ACL] = attrs[RGW_ATTR_ACL];
  }
}

class GetObjHandleDestructor {
  RGWRados *store;
  void **handle;

public:
    GetObjHandleDestructor(RGWRados *_store) : store(_store), handle(NULL) {}
    ~GetObjHandleDestructor() {
      if (handle) {
	store->finish_get_obj(handle);
      }
    }
    void set_handle(void **_h) {
      handle = _h;
    }
};

/**
 * Copy an object.
 * dest_obj: the object to copy into
 * src_obj: the object to copy from
 * attrs: if replace_attrs is set then these are placed on the new object
 * err: stores any errors resulting from the get of the original object
 * Returns: 0 on success, -ERR# otherwise.
 */
int RGWRados::copy_obj(void *ctx,
	       const string& user_id,
	       const string& client_id,
	       const string& op_id,
	       req_info *info,
	       const string& source_zone,
	       rgw_obj& dest_obj,
	       rgw_obj& src_obj,
	       RGWBucketInfo& dest_bucket_info,
	       RGWBucketInfo& src_bucket_info,
	       time_t *mtime,
	       const time_t *mod_ptr,
	       const time_t *unmod_ptr,
	       const char *if_match,
	       const char *if_nomatch,
	       bool replace_attrs,
	       map<string, bufferlist>& attrs,
	       RGWObjCategory category,
	       string *ptag,
	       struct rgw_err *err,
	       void (*progress_cb)(off_t, void *),
	       void *progress_data)
{
  int ret;
  uint64_t total_len, obj_size;
  time_t lastmod;
  rgw_obj shadow_obj = dest_obj;
  string shadow_oid;

  bool remote_src;
  bool remote_dest;

  append_rand_alpha(cct, dest_obj.object, shadow_oid, 32);

  remote_dest = !region.equals(dest_bucket_info.region);
  remote_src = !region.equals(src_bucket_info.region);

  if (remote_src && remote_dest) {
    ldout(cct, 0) << "ERROR: can't copy object when both src and dest buckets are remote" << dendl;
    return -EINVAL;
  }

  ldout(cct, 5) << "Copy object " << src_obj.bucket << ":" << src_obj.object << " => " << dest_obj.bucket << ":" << dest_obj.object << dendl;

  void *handle = NULL;
  GetObjHandleDestructor handle_destructor(this);

  map<string, bufferlist> src_attrs;
  off_t ofs = 0;
  off_t end = -1;
  if (!remote_src && source_zone.empty()) {
    ret = prepare_get_obj(ctx, src_obj, &ofs, &end, &src_attrs,
		  mod_ptr, unmod_ptr, &lastmod, if_match, if_nomatch, &total_len, &obj_size, NULL, &handle, err);
    if (ret < 0)
      return ret;

    handle_destructor.set_handle(&handle);
  } else {
    /* source is in a different region, copy it there */

    RGWRESTStreamReadRequest *in_stream_req;
    string tag;
    append_rand_alpha(cct, tag, tag, 32);

    RGWPutObjProcessor_Atomic processor(dest_bucket_info.owner, dest_obj.bucket, dest_obj.object,
					cct->_conf->rgw_obj_stripe_size, tag);
    ret = processor.prepare(this, ctx);
    if (ret < 0)
      return ret;

    RGWRESTConn *conn;
    if (source_zone.empty()) {
      if (dest_bucket_info.region.empty()) {
	/* source is in the master region */
	conn = rest_master_conn;
      } else {
	map<string, RGWRESTConn *>::iterator iter = region_conn_map.find(src_bucket_info.region);
	if (iter == region_conn_map.end()) {
	  ldout(cct, 0) << "could not find region connection to region: " << source_zone << dendl;
	  return -ENOENT;
	}
	conn = iter->second;
      }
    } else {
      map<string, RGWRESTConn *>::iterator iter = zone_conn_map.find(source_zone);
      if (iter == zone_conn_map.end()) {
	ldout(cct, 0) << "could not find zone connection to zone: " << source_zone << dendl;
	return -ENOENT;
      }
      conn = iter->second;
    }

    string obj_name = dest_obj.bucket.name + "/" + dest_obj.object;

    RGWOpStateSingleOp opstate(this, client_id, op_id, obj_name);

    int ret = opstate.set_state(RGWOpState::OPSTATE_IN_PROGRESS);
    if (ret < 0) {
      ldout(cct, 0) << "ERROR: failed to set opstate ret=" << ret << dendl;
      return ret;
    }
    RGWRadosPutObj cb(&processor, &opstate, progress_cb, progress_data);
    string etag;
    map<string, string> req_headers;
    time_t set_mtime;

    ret = conn->get_obj(user_id, info, src_obj, true, &cb, &in_stream_req);
    if (ret < 0)
      goto set_err_state;

    ret = conn->complete_request(in_stream_req, etag, &set_mtime, req_headers);
    if (ret < 0)
      goto set_err_state;

    { /* opening scope so that we can do goto, sorry */
      bufferlist& extra_data_bl = processor.get_extra_data();
      if (extra_data_bl.length()) {
	JSONParser jp;
	if (!jp.parse(extra_data_bl.c_str(), extra_data_bl.length())) {
	  ldout(cct, 0) << "failed to parse response extra data. len=" << extra_data_bl.length() << " data=" << extra_data_bl.c_str() << dendl;
	  goto set_err_state;
	}

	JSONDecoder::decode_json("attrs", src_attrs, &jp);

	src_attrs.erase(RGW_ATTR_MANIFEST); // not interested in original object layout
      }
    }

    set_copy_attrs(src_attrs, attrs, replace_attrs, !source_zone.empty());

    ret = cb.complete(etag, mtime, set_mtime, src_attrs);
    if (ret < 0)
      goto set_err_state;

    ret = opstate.set_state(RGWOpState::OPSTATE_COMPLETE);
    if (ret < 0) {
      ldout(cct, 0) << "ERROR: failed to set opstate ret=" << ret << dendl;
    }

    return 0;
set_err_state:
    int r = opstate.set_state(RGWOpState::OPSTATE_ERROR);
    if (r < 0) {
      ldout(cct, 0) << "ERROR: failed to set opstate r=" << ret << dendl;
    }
    return ret;
  }
  set_copy_attrs(src_attrs, attrs, replace_attrs, false);
  src_attrs.erase(RGW_ATTR_ID_TAG);

  RGWObjManifest manifest;
  RGWObjState *astate = NULL;
  RGWRadosCtx *rctx = static_cast<RGWRadosCtx *>(ctx);
  ret = get_obj_state(rctx, src_obj, &astate, NULL);
  if (ret < 0)
    return ret;

  vector<rgw_obj> ref_objs;

  bool copy_data = !astate->has_manifest;
  bool copy_first = false;
  if (astate->has_manifest) {
    if (!astate->manifest.has_tail()) {
      copy_data = true;
    } else {
      uint64_t head_size = astate->manifest.get_head_size();

      if (head_size > 0) {
	if (head_size > max_chunk_size)	 // should never happen
	  copy_data = true;
	else
	  copy_first = true;
      }
    }
  }


  if (remote_dest) {
    /* dest is in a different region, copy it there */

    string etag;

    RGWRESTStreamWriteRequest *out_stream_req;

    int ret = rest_master_conn->put_obj_init(user_id, dest_obj, astate->size, src_attrs, &out_stream_req);
    if (ret < 0)
      return ret;

    ret = get_obj_iterate(ctx, &handle, src_obj, 0, astate->size - 1, out_stream_req->get_out_cb());
    if (ret < 0)
      return ret;

    ret = rest_master_conn->complete_request(out_stream_req, etag, mtime);
    if (ret < 0)
      return ret;

    return 0;
  } else if (copy_data) { /* refcounting tail wouldn't work here, just copy the data */
    return copy_obj_data(ctx, dest_bucket_info.owner, &handle, end, dest_obj, src_obj, mtime, src_attrs, category, ptag, err);
  }

  RGWObjManifest::obj_iterator miter = astate->manifest.obj_begin();

  if (copy_first) // we need to copy first chunk, not increase refcount
    ++miter;

  rgw_rados_ref ref;
  rgw_bucket bucket;
  ret = get_obj_ref(miter.get_location(), &ref, &bucket);
  if (ret < 0) {
    return ret;
  }
  PutObjMetaExtraParams ep;

  bufferlist first_chunk;

  bool copy_itself = (dest_obj == src_obj);
  RGWObjManifest *pmanifest;
  ldout(cct, 0) << "dest_obj=" << dest_obj << " src_obj=" << src_obj << " copy_itself=" << (int)copy_itself << dendl;


  string tag;

  if (ptag)
    tag = *ptag;

  if (tag.empty()) {
    append_rand_alpha(cct, tag, tag, 32);
  }

  if (!copy_itself) {
    manifest = astate->manifest;
    rgw_bucket& tail_bucket = manifest.get_tail_bucket();
    if (tail_bucket.name.empty()) {
      manifest.set_tail_bucket(src_obj.bucket);
    }
    string oid;
    for (; miter != astate->manifest.obj_end(); ++miter) {
      ObjectOperation op(ref.vol->op());
      cls_refcount_get(op, tag, true);
      const rgw_obj& loc = miter.get_location();
      get_obj_bucket_and_oid(loc, bucket, oid);

      ret = rc.objecter->mutate(oid, ref.vol, op);
      if (ret < 0)
	goto done_ret;

      ref_objs.push_back(loc);
    }

    pmanifest = &manifest;
  } else {
    pmanifest = &astate->manifest;
    /* don't send the object's tail for garbage collection */
    astate->keep_tail = true;
  }

  if (copy_first) {
    ret = get_obj(ctx, NULL, &handle, src_obj, first_chunk, 0, max_chunk_size);
    if (ret < 0)
      goto done_ret;

    pmanifest->set_head(dest_obj);
    pmanifest->set_head_size(first_chunk.length());
  }

  ep.data = &first_chunk;
  ep.manifest = pmanifest;
  ep.ptag = &tag;
  ep.owner = dest_bucket_info.owner;

  ret = put_obj_meta(ctx, dest_obj, end + 1, src_attrs, category, PUT_OBJ_CREATE, ep);

  if (mtime)
    obj_stat(ctx, dest_obj, NULL, mtime, NULL, NULL, NULL);

  return 0;

done_ret:
  if (!copy_itself) {
    vector<rgw_obj>::iterator riter;

    string oid;

    /* rollback reference */
    for (riter = ref_objs.begin(); riter != ref_objs.end(); ++riter) {
      ObjectOperation op(ref.vol->op());
      cls_refcount_put(op, tag, true);

      get_obj_bucket_and_oid(*riter, bucket, oid);

      int r = rc.objecter->mutate(oid, ref.vol, op);
      if (r < 0) {
	ldout(cct, 0) << "ERROR: cleanup after error failed to drop reference on oid=" << *riter << dendl;
      }
    }
  }
  return ret;
}


int RGWRados::copy_obj_data(void *ctx,
	       const string& owner,
	       void **handle, off_t end,
	       rgw_obj& dest_obj,
	       rgw_obj& src_obj,
	       time_t *mtime,
	       map<string, bufferlist>& attrs,
	       RGWObjCategory category,
	       string *ptag,
	       struct rgw_err *err)
{
  bufferlist first_chunk;
  RGWObjManifest manifest;
  map<uint64_t, RGWObjManifestPart> objs;
  RGWObjManifestPart *first_part;
  map<string, bufferlist>::iterator iter;

  rgw_obj shadow_obj = dest_obj;
  string shadow_oid;

  append_rand_alpha(cct, dest_obj.object, shadow_oid, 32);

  int ret, r;
  off_t ofs = 0;
  PutObjMetaExtraParams ep;

  do {
    bufferlist bl;
    ret = get_obj(ctx, NULL, handle, src_obj, bl, ofs, end);
    if (ret < 0)
      return ret;

    const char *data = bl.c_str();

    if ((uint64_t)ofs < max_chunk_size) {
      uint64_t len = min(max_chunk_size - ofs, (uint64_t)ret);
      first_chunk.append(data, len);
      ofs += len;
      ret -= len;
      data += len;
    }

    // In the first call to put_obj_data, we pass ofs == -1 so that it will do
    // a write_full, wiping out whatever was in the object before this
    r = 0;
    if (ret > 0) {
      r = put_obj_data(ctx, shadow_obj, data, ((ofs == 0) ? -1 : ofs), ret, false);
    }
    if (r < 0)
      goto done_err;

    ofs += ret;
  } while (ofs <= end);

  first_part = &objs[0];
  first_part->loc = dest_obj;
  first_part->loc_ofs = 0;
  first_part->size = first_chunk.length();

  if ((uint64_t)ofs > max_chunk_size) {
    RGWObjManifestPart& tail = objs[max_chunk_size];
    tail.loc = shadow_obj;
    tail.loc_ofs = max_chunk_size;
    tail.size = ofs - max_chunk_size;
  }

  manifest.set_explicit(ofs, objs);

  ep.data = &first_chunk;
  ep.manifest = &manifest;
  ep.ptag = ptag;
  ep.owner = owner;

  ret = put_obj_meta(ctx, dest_obj, end + 1, attrs, category, PUT_OBJ_CREATE, ep);
  if (mtime)
    obj_stat(ctx, dest_obj, NULL, mtime, NULL, NULL, NULL);

  return ret;
done_err:
  delete_obj(ctx, owner, shadow_obj);
  return r;
}

/**
 * Delete a bucket.
 * bucket: the name of the bucket to delete
 * Returns 0 on success, -ERR# otherwise.
 */
int RGWRados::delete_bucket(rgw_bucket& bucket,
			    RGWObjVersionTracker& objv_tracker)
{
  VolumeRef index_vol;
  string oid;
  int r = open_bucket_index(bucket, index_vol, oid);
  if (r < 0)
    return r;

  std::map<string, RGWObjEnt> ent_map;
  string marker, prefix;
  bool is_truncated;

  do {
#define NUM_ENTRIES 1000
    r = cls_bucket_list(bucket, marker, prefix, NUM_ENTRIES, ent_map,
			&is_truncated, &marker);
    if (r < 0)
      return r;

    std::map<string, RGWObjEnt>::iterator eiter;
    string oid;
    for (eiter = ent_map.begin(); eiter != ent_map.end(); ++eiter) {
      oid = eiter->first;
    }
  } while (is_truncated);

  r = rgw_bucket_delete_bucket_obj(this, bucket.name, objv_tracker);
  if (r < 0)
    return r;

  return 0;
}


int RGWRados::set_bucket_owner(rgw_bucket& bucket, ACLOwner& owner)
{
  RGWBucketInfo info;
  map<string, bufferlist> attrs;
  int r = get_bucket_info(NULL, bucket.name, info, NULL, &attrs);
  if (r < 0) {
    ldout(cct, 0) << "NOTICE: get_bucket_info on bucket=" << bucket.name << " returned err=" << r << dendl;
    return r;
  }

  info.owner = owner.get_id();

  r = put_bucket_instance_info(info, false, 0, &attrs);
  if (r < 0) {
    ldout(cct, 0) << "NOTICE: put_bucket_info on bucket=" << bucket.name << " returned err=" << r << dendl;
    return r;
  }

  return 0;
}


int RGWRados::set_buckets_enabled(vector<rgw_bucket>& buckets, bool enabled)
{
  int ret = 0;

  vector<rgw_bucket>::iterator iter;

  for (iter = buckets.begin(); iter != buckets.end(); ++iter) {
    rgw_bucket& bucket = *iter;
    if (enabled)
      ldout(cct, 20) << "enabling bucket name=" << bucket.name << dendl;
    else
      ldout(cct, 20) << "disabling bucket name=" << bucket.name << dendl;

    RGWBucketInfo info;
    map<string, bufferlist> attrs;
    int r = get_bucket_info(NULL, bucket.name, info, NULL, &attrs);
    if (r < 0) {
      ldout(cct, 0) << "NOTICE: get_bucket_info on bucket=" << bucket.name << " returned err=" << r << ", skipping bucket" << dendl;
      ret = r;
      continue;
    }
    if (enabled) {
      info.flags &= ~BUCKET_SUSPENDED;
    } else {
      info.flags |= BUCKET_SUSPENDED;
    }

    r = put_bucket_instance_info(info, false, 0, &attrs);
    if (r < 0) {
      ldout(cct, 0) << "NOTICE: put_bucket_info on bucket=" << bucket.name << " returned err=" << r << ", skipping bucket" << dendl;
      ret = r;
      continue;
    }
  }
  return ret;
}

int RGWRados::bucket_suspended(rgw_bucket& bucket, bool *suspended)
{
  RGWBucketInfo bucket_info;
  int ret = get_bucket_info(NULL, bucket.name, bucket_info, NULL);
  if (ret < 0) {
    return ret;
  }

  *suspended = ((bucket_info.flags & BUCKET_SUSPENDED) != 0);
  return 0;
}

int RGWRados::complete_atomic_overwrite(RGWRadosCtx *rctx, RGWObjState *state, rgw_obj& oid)
{
  if (!state || !state->has_manifest || state->keep_tail)
    return 0;

  cls_rgw_obj_chain chain;
  RGWObjManifest::obj_iterator iter;
  for (iter = state->manifest.obj_begin(); iter != state->manifest.obj_end(); ++iter) {
    const rgw_obj& mobj = iter.get_location();
    if (mobj == oid)
      continue;
    string oid;
    rgw_bucket bucket;
    get_obj_bucket_and_oid(mobj, bucket, oid);
    chain.push_obj(bucket.data_vol, oid);
  }

  string tag = state->obj_tag.c_str();
  int ret = gc->send_chain(chain, tag, false);	// do it async

  return ret;
}

int RGWRados::open_bucket_index(rgw_bucket& bucket, VolumeRef& index_vol,
				string& bucket_oid)
{
  if (bucket_is_system(bucket))
    return -EINVAL;

  int r = open_bucket_index_vol(bucket, index_vol);
  if (r < 0)
    return r;

  if (bucket.marker.empty()) {
    ldout(cct, 0) << "ERROR: empty marker for bucket operation" << dendl;
    return -EIO;
  }

  bucket_oid = dir_oid_prefix;
  bucket_oid.append(bucket.marker);

  return 0;
}

static void translate_raw_stats(rgw_bucket_dir_header& header,
				map<RGWObjCategory, RGWStorageStats>& stats)
{
  map<uint8_t, struct rgw_bucket_category_stats>::iterator iter
    = header.stats.begin();
  for (; iter != header.stats.end(); ++iter) {
    RGWObjCategory category = (RGWObjCategory)iter->first;
    RGWStorageStats& s = stats[category];
    struct rgw_bucket_category_stats& header_stats = iter->second;
    s.category = (RGWObjCategory)iter->first;
    s.num_kb = ((header_stats.total_size + 1023) / 1024);
    s.num_kb_rounded = ((header_stats.total_size_rounded + 1023) / 1024);
    s.num_objects = header_stats.num_entries;
  }
}

int RGWRados::bucket_check_index(
  rgw_bucket& bucket,
  map<RGWObjCategory, RGWStorageStats> *existing_stats,
  map<RGWObjCategory, RGWStorageStats> *calculated_stats)
{
  VolumeRef index_vol;
  string oid;

  int ret = open_bucket_index(bucket, index_vol, oid);
  if (ret < 0)
    return ret;

  rgw_bucket_dir_header existing_header;
  rgw_bucket_dir_header calculated_header;

  ret = cls_rgw_bucket_check_index_op(rc.objecter, index_vol, oid,
				      &existing_header, &calculated_header);
  if (ret < 0)
    return ret;

  translate_raw_stats(existing_header, *existing_stats);
  translate_raw_stats(calculated_header, *calculated_stats);

  return 0;
}

int RGWRados::bucket_rebuild_index(rgw_bucket& bucket)
{
  VolumeRef index_vol;
  string oid;

  int ret = open_bucket_index(bucket, index_vol, oid);
  if (ret < 0)
    return ret;

  return cls_rgw_bucket_rebuild_index_op(rc.objecter, index_vol, oid);
}


int RGWRados::defer_gc(void *ctx, rgw_obj& obj)
{
  RGWRadosCtx *rctx = static_cast<RGWRadosCtx *>(ctx);
  rgw_bucket bucket;
  std::string oid;
  get_obj_bucket_and_oid(obj, bucket, oid);
  if (!rctx)
    return 0;

  RGWObjState *state = NULL;

  int r = get_obj_state(rctx, obj, &state, NULL);
  if (r < 0)
    return r;

  if (!state->is_atomic) {
    ldout(cct, 20) << "state for oid=" << oid
		   << " is not atomic, not deferring gc operation" << dendl;
    return -EINVAL;
  }

  if (state->obj_tag.length() == 0) {// check for backward compatibility
    ldout(cct, 20) << "state->obj_tag is empty, not deferring gc operation" << dendl;
    return -EINVAL;
  }

  string tag = state->obj_tag.c_str();

  ldout(cct, 0) << "defer chain tag=" << tag << dendl;

  return gc->defer_chain(tag, false);
}


/**
 * Delete an object.
 * bucket: name of the bucket storing the object
 * oid: name of the object to delete
 * Returns: 0 on success, -ERR# otherwise.
 */
int RGWRados::delete_obj_impl(void *ctx, const string& bucket_owner,
			      rgw_obj& oid, RGWObjVersionTracker *objv_tracker)
{
  rgw_rados_ref ref;
  rgw_bucket bucket;
  int r = get_obj_ref(oid, &ref, &bucket);
  if (r < 0) {
    return r;
  }
  RGWRadosCtx *rctx = static_cast<RGWRadosCtx *>(ctx);

  ObjectOperation op(ref.vol->op());

  RGWObjState *state;
  r = prepare_atomic_for_write(rctx, oid, op, &state, false, NULL);
  if (r < 0)
    return r;

  bool ret_not_existed = (state && !state->exists);

  string tag;
  r = prepare_update_index(state, bucket, CLS_RGW_OP_DEL, oid, tag);
  if (r < 0)
    return r;

  if (objv_tracker) {
    objv_tracker->prepare_op_for_write(op);
  }

  cls_refcount_put(op, tag, true);
  r = rc.objecter->mutate(ref.oid, ref.vol, op);
  bool removed = (r >= 0);

  const auto& volid = ref.vol->id;
  if (r >= 0 || r == -ENOENT) {
    r = complete_update_index_del(bucket, oid.object, tag, volid);
  } else {
    int ret = complete_update_index_cancel(bucket, oid.object, tag);
    if (ret < 0) {
      ldout(cct, 0) << "ERROR: complete_update_index_cancel returned ret="
		    << ret << dendl;
    }
  }
  if (removed) {
    int ret = complete_atomic_overwrite(rctx, state, oid);
    if (ret < 0) {
      ldout(cct, 0) << "ERROR: complete_atomic_removal returned ret=" << ret << dendl;
    }
    /* other than that, no need to propagate error */
  }

  atomic_write_finish(state, r);

  if (r < 0)
    return r;

  if (ret_not_existed)
    return -ENOENT;

  if (state) {
    /* update quota cache */
    quota_handler->update_stats(bucket_owner, bucket, -1, 0, state->size);
  }

  return 0;
}

int RGWRados::delete_obj(void *ctx, const string& bucket_owner, rgw_obj& oid, RGWObjVersionTracker *objv_tracker)
{
  int r;

  r = delete_obj_impl(ctx, bucket_owner, oid, objv_tracker);
  if (r == -ECANCELED)
    r = 0;

  return r;
}

int RGWRados::delete_system_obj(void *ctx, rgw_obj& oid, RGWObjVersionTracker *objv_tracker)
{
  int r;

  string no_owner;
  r = delete_obj_impl(ctx, no_owner, oid, objv_tracker);
  if (r == -ECANCELED)
    r = 0;

  return r;
}

int RGWRados::delete_obj_index(rgw_obj& obj)
{
  rgw_bucket bucket;
  std::string oid;
  get_obj_bucket_and_oid(obj, bucket, oid);

  string tag;
  int r = complete_update_index_del(bucket, obj.object, tag,
				    boost::uuids::nil_uuid());

  return r;
}

static void generate_fake_tag(CephContext *cct,
			      map<string, bufferlist>& attrset,
			      RGWObjManifest& manifest,
			      bufferlist& manifest_bl, bufferlist& tag_bl)
{
  string tag;

  RGWObjManifest::obj_iterator mi = manifest.obj_begin();
  if (mi != manifest.obj_end()) {
    if (manifest.has_tail()) // first object usually points at the head, let's skip to a more unique part
      ++mi;
    tag = mi.get_location().object;
    tag.append("_");
  }

  unsigned char md5[CEPH_CRYPTO_MD5_DIGESTSIZE];
  char md5_str[CEPH_CRYPTO_MD5_DIGESTSIZE * 2 + 1];
  MD5 hash;
  hash.Update((const byte *)manifest_bl.c_str(), manifest_bl.length());

  map<string, bufferlist>::iterator iter = attrset.find(RGW_ATTR_ETAG);
  if (iter != attrset.end()) {
    bufferlist& bl = iter->second;
    hash.Update((const byte *)bl.c_str(), bl.length());
  }

  hash.Final(md5);
  buf_to_hex(md5, CEPH_CRYPTO_MD5_DIGESTSIZE, md5_str);
  tag.append(md5_str);

  ldout(cct, 10) << "generate_fake_tag new tag=" << tag << dendl;

  tag_bl.append(tag.c_str(), tag.size() + 1);
}

int RGWRados::get_obj_state(RGWRadosCtx *rctx, rgw_obj& oid, RGWObjState **state, RGWObjVersionTracker *objv_tracker)
{
  RGWObjState *s = rctx->get_state(oid);
  ldout(cct, 20) << "get_obj_state: rctx=" << (void *)rctx << " oid=" << oid
		 << " state=" << (void *)s << " s->prefetch_data="
		 << s->prefetch_data << dendl;
  *state = s;
  if (s->has_attrs)
    return 0;

  int r = obj_stat(rctx, oid, &s->size, &s->mtime, &s->attrset,
		   (s->prefetch_data ? &s->data : NULL), objv_tracker);
  if (r == -ENOENT) {
    s->exists = false;
    s->has_attrs = true;
    s->mtime = 0;
    return 0;
  }
  if (r < 0)
    return r;

  s->exists = true;
  s->has_attrs = true;
  map<string, bufferlist>::iterator iter = s->attrset.find(RGW_ATTR_SHADOW_OBJ);
  if (iter != s->attrset.end()) {
    bufferlist bl = iter->second;
    bufferlist::iterator it = bl.begin();
    it.copy(bl.length(), s->shadow_obj);
    s->shadow_obj[bl.length()] = '\0';
  }
  s->obj_tag = s->attrset[RGW_ATTR_ID_TAG];
  bufferlist manifest_bl = s->attrset[RGW_ATTR_MANIFEST];
  if (manifest_bl.length()) {
    bufferlist::iterator miter = manifest_bl.begin();
    try {
      ::decode(s->manifest, miter);
      s->has_manifest = true;
      s->size = s->manifest.get_obj_size();
    } catch (buffer::error& err) {
      ldout(cct, 20) << "ERROR: couldn't decode manifest" << dendl;
      return -EIO;
    }
    ldout(cct, 10) << "manifest: total_size = " << s->manifest.get_obj_size() << dendl;
    if (cct->_conf->subsys.should_gather(ceph_subsys_rgw, 20) && s->manifest.has_explicit_objs()) {
      RGWObjManifest::obj_iterator mi;
      for (mi = s->manifest.obj_begin(); mi != s->manifest.obj_end(); ++mi) {
	ldout(cct, 20) << "manifest: ofs=" << mi.get_ofs() << " loc=" << mi.get_location() << dendl;
      }
    }

    if (!s->obj_tag.length()) {
      /*
       * Uh oh, something's wrong, object with manifest should have tag. Let's
       * create one out of the manifest, would be unique
       */
      generate_fake_tag(cct, s->attrset, s->manifest, manifest_bl, s->obj_tag);
      s->fake_tag = true;
    }
  }
  if (s->obj_tag.length())
    ldout(cct, 20) << "get_obj_state: setting s->obj_tag to " << s->obj_tag.c_str() << dendl;
  else
    ldout(cct, 20) << "get_obj_state: s->obj_tag was set empty" << dendl;
  return 0;
}

/**
 * Get the attributes for an object.
 * bucket: name of the bucket holding the object.
 * oid: name of the object
 * name: name of the attr to retrieve
 * dest: bufferlist to store the result in
 * Returns: 0 on success, -ERR# otherwise.
 */
int RGWRados::get_attr(void *ctx, rgw_obj& oid, const char *name, bufferlist& dest)
{
  rgw_rados_ref ref;
  rgw_bucket bucket;
  int r = get_obj_ref(oid, &ref, &bucket, true);
  if (r < 0) {
    return r;
  }
  RGWRadosCtx *rctx = static_cast<RGWRadosCtx *>(ctx);

  if (rctx) {
    RGWObjState *state;
    r = get_obj_state(rctx, oid, &state, NULL);
    if (r < 0)
      return r;
    if (!state->exists)
      return -ENOENT;
    if (state->get_attr(name, dest))
      return 0;
    return -ENODATA;
  }

  ObjectOperation op(ref.vol->op());

  int rval;
  op->getxattr(name, &dest, &rval);

  r = rc.objecter->read(ref.oid, ref.vol, op);
  if (r < 0)
    return r;

  return 0;
}

int RGWRados::append_atomic_test(RGWRadosCtx *rctx, rgw_obj& oid,
				 ObjOpUse op, RGWObjState **pstate)
{
  if (!rctx)
    return 0;

  int r = get_obj_state(rctx, oid, pstate, NULL);
  if (r < 0)
    return r;

  RGWObjState *state = *pstate;

  if (!state->is_atomic) {
    ldout(cct, 20) << "state for oid=" << oid
		   << " is not atomic, not appending atomic test" << dendl;
    return 0;
  }

  if (state->obj_tag.length() > 0 && !state->fake_tag) {
    // check for backward compatibility
    op->cmpxattr(RGW_ATTR_ID_TAG, CEPH_OSD_CMPXATTR_OP_EQ,
		 CEPH_OSD_CMPXATTR_MODE_STRING, state->obj_tag);
  } else {
    ldout(cct, 20) << "state->obj_tag is empty, not appending atomic test"
		   << dendl;
  }
  return 0;
}

int RGWRados::prepare_atomic_for_write_impl(RGWRadosCtx *rctx, rgw_obj& oid,
			    ObjOpUse op, RGWObjState **pstate,
			    bool reset_obj, const string *ptag)
{
  int r = get_obj_state(rctx, oid, pstate, NULL);
  if (r < 0)
    return r;

  RGWObjState *state = *pstate;

  bool need_guard = (state->has_manifest || (state->obj_tag.length() != 0)) && (!state->fake_tag);

  if (!state->is_atomic) {
    ldout(cct, 20) << "prepare_atomic_for_write_impl: state is not atomic. state=" << (void *)state << dendl;

    if (reset_obj) {
      op->create(false);
      op->remove(); // we're not dropping reference here, actually removing object
    }

    return 0;
  }

  if (need_guard) {
    /* first verify that the object wasn't replaced under */
    op->cmpxattr(RGW_ATTR_ID_TAG, CEPH_OSD_CMPXATTR_OP_EQ,
		 CEPH_OSD_CMPXATTR_MODE_STRING, state->obj_tag);
    // FIXME: need to add FAIL_NOTEXIST_OK for racing deletion
  }

  if (reset_obj) {
    if (state->exists) {
      op->create(false);
      op->remove();
    } else {
      op->create(true);
    }
  }

  if (ptag) {
    state->write_tag = *ptag;
  } else {
    append_rand_alpha(cct, state->write_tag, state->write_tag, 32);
  }
  bufferlist bl;
  bl.append(state->write_tag.c_str(), state->write_tag.size() + 1);

  ldout(cct, 10) << "setting object write_tag=" << state->write_tag << dendl;

  op->setxattr(RGW_ATTR_ID_TAG, bl);

  return 0;
}

int RGWRados::prepare_atomic_for_write(RGWRadosCtx *rctx, rgw_obj& oid,
			    ObjOpUse op, RGWObjState **pstate,
			    bool reset_obj, const string *ptag)
{
  if (!rctx) {
    *pstate = NULL;
    return 0;
  }

  int r;
  r = prepare_atomic_for_write_impl(rctx, oid, op, pstate, reset_obj, ptag);

  return r;
}

/**
 * Set an attr on an object.
 * bucket: name of the bucket holding the object
 * oid: name of the object to set the attr on
 * name: the attr to set
 * bl: the contents of the attr
 * Returns: 0 on success, -ERR# otherwise.
 */
int RGWRados::set_attr(void *ctx, rgw_obj& oid, const char *name, bufferlist& bl, RGWObjVersionTracker *objv_tracker)
{
  map<string, bufferlist> attrs;
  attrs[name] = bl;
  return set_attrs(ctx, oid, attrs, NULL, objv_tracker);
}

int RGWRados::set_attrs(void *ctx, rgw_obj& oid,
			map<string, bufferlist>& attrs,
			map<string, bufferlist>* rmattrs,
			RGWObjVersionTracker *objv_tracker)
{
  rgw_rados_ref ref;
  rgw_bucket bucket;
  int r = get_obj_ref(oid, &ref, &bucket, true);
  if (r < 0) {
    return r;
  }
  RGWRadosCtx *rctx = static_cast<RGWRadosCtx *>(ctx);

  ObjectOperation op(ref.vol->op());
  RGWObjState *state = NULL;

  r = append_atomic_test(rctx, oid, op, &state);
  if (r < 0)
    return r;

  if (objv_tracker) {
    objv_tracker->prepare_op_for_write(op);
  }

  map<string, bufferlist>::iterator iter;
  if (rmattrs) {
    for (iter = rmattrs->begin(); iter != rmattrs->end(); ++iter) {
      const string& name = iter->first;
      op->rmxattr(name.c_str());
    }
  }

  for (iter = attrs.begin(); iter != attrs.end(); ++iter) {
    const string& name = iter->first;
    bufferlist& bl = iter->second;

    if (!bl.length())
      continue;

    op->setxattr(name, bl);
  }

  if (!op->size())
    return 0;

  r = rc.objecter->mutate(ref.oid, ref.vol, op);
  if (r < 0)
    return r;

  if (state) {
    if (rmattrs) {
      for (iter = rmattrs->begin(); iter != rmattrs->end(); ++iter) {
	state->attrset.erase(iter->first);
      }
    }
    for (iter = attrs.begin(); iter != attrs.end(); ++iter) {
      state->attrset[iter->first] = iter->second;
    }
  }

  return 0;
}

/**
 * Get data about an object out of RADOS and into memory.
 * bucket: name of the bucket the object is in.
 * oid: name of the object to read
 * data: if get_data==true, this pointer will be set
 *    to an address containing the object's data/value
 * ofs: the offset of the object to read from
 * end: the point in the object to stop reading
 * attrs: if non-NULL, the pointed-to map will contain
 *    all the attrs of the object when this function returns
 * mod_ptr: if non-NULL, compares the object's mtime to *mod_ptr,
 *    and if mtime is smaller it fails.
 * unmod_ptr: if non-NULL, compares the object's mtime to *unmod_ptr,
 *    and if mtime is >= it fails.
 * if_match/nomatch: if non-NULL, compares the object's etag attr
 *    to the string and, if it doesn't/does match, fails out.
 * get_data: if true, the object's data/value will be read out, otherwise not
 * err: Many errors will result in this structure being filled
 *    with extra informatin on the error.
 * Returns: -ERR# on failure, otherwise
 *	    (if get_data==true) length of read data,
 *	    (if get_data==false) length of the object
 */
int RGWRados::prepare_get_obj(void *ctx, rgw_obj& oid,
			      off_t *pofs, off_t *pend,
			      map<string, bufferlist> *attrs,
			      const time_t *mod_ptr,
			      const time_t *unmod_ptr,
			      time_t *lastmod,
			      const char *if_match,
			      const char *if_nomatch,
			      uint64_t *total_size,
			      uint64_t *obj_size,
			      RGWObjVersionTracker *objv_tracker,
			      void **handle,
			      struct rgw_err *err)
{
  bufferlist etag;
  time_t ctime;
  RGWRadosCtx *rctx = static_cast<RGWRadosCtx *>(ctx);
  RGWRadosCtx *new_ctx = NULL;
  RGWObjState *astate = NULL;
  off_t ofs = 0;
  off_t end = -1;

  map<string, bufferlist>::iterator iter;

  *handle = NULL;

  GetObjState *state = new GetObjState;
  if (!state)
    return -ENOMEM;

  *handle = state;

  int r = get_obj_vol(oid, state->vol);
  if (r < 0) {
    delete state;
    return r;
  }

  if (!rctx) {
    new_ctx = new RGWRadosCtx(this);
    rctx = new_ctx;
  }

  r = get_obj_state(rctx, oid, &astate, objv_tracker);
  if (r < 0)
    goto done_err;

  if (!astate->exists) {
    r = -ENOENT;
    goto done_err;
  }

  if (attrs) {
    *attrs = astate->attrset;
    if (cct->_conf->subsys.should_gather(ceph_subsys_rgw, 20)) {
      for (iter = attrs->begin(); iter != attrs->end(); ++iter) {
	ldout(cct, 20) << "Read xattr: " << iter->first << dendl;
      }
    }
    if (r < 0)
      goto done_err;
  }

  /* Convert all times go GMT to make them compatible */
  if (mod_ptr || unmod_ptr) {
    ctime = astate->mtime;

    if (mod_ptr) {
      ldout(cct, 10) << "If-Modified-Since: " << *mod_ptr << " Last-Modified: " << ctime << dendl;
      if (ctime < *mod_ptr) {
	r = -ERR_NOT_MODIFIED;
	goto done_err;
      }
    }

    if (unmod_ptr) {
      ldout(cct, 10) << "If-UnModified-Since: " << *unmod_ptr << " Last-Modified: " << ctime << dendl;
      if (ctime > *unmod_ptr) {
	r = -ERR_PRECONDITION_FAILED;
	goto done_err;
      }
    }
  }
  if (if_match || if_nomatch) {
    r = get_attr(rctx, oid, RGW_ATTR_ETAG, etag);
    if (r < 0)
      goto done_err;

    if (if_match) {
      string if_match_str = rgw_string_unquote(if_match);
      ldout(cct, 10) << "ETag: " << etag.c_str() << " " << " If-Match: " << if_match_str << dendl;
      if (if_match_str.compare(etag.c_str()) != 0) {
	r = -ERR_PRECONDITION_FAILED;
	goto done_err;
      }
    }

    if (if_nomatch) {
      string if_nomatch_str = rgw_string_unquote(if_nomatch);
      ldout(cct, 10) << "ETag: " << etag.c_str() << " " << " If-NoMatch: " << if_nomatch_str << dendl;
      if (if_nomatch_str.compare(etag.c_str()) == 0) {
	r = -ERR_NOT_MODIFIED;
	goto done_err;
      }
    }
  }

  if (pofs)
    ofs = *pofs;
  if (pend)
    end = *pend;

  if (ofs < 0) {
    ofs += astate->size;
    if (ofs < 0)
      ofs = 0;
    end = astate->size - 1;
  } else if (end < 0) {
    end = astate->size - 1;
  }

  if (astate->size > 0) {
    if (ofs >= (off_t)astate->size) {
      r = -ERANGE;
      goto done_err;
    }
    if (end >= (off_t)astate->size) {
      end = astate->size - 1;
    }
  }

  if (pofs)
    *pofs = ofs;
  if (pend)
    *pend = end;
  if (total_size)
    *total_size = (ofs <= end ? end + 1 - ofs : 0);
  if (obj_size)
    *obj_size = astate->size;
  if (lastmod)
    *lastmod = astate->mtime;

  delete new_ctx;

  return 0;

done_err:
  delete new_ctx;
  finish_get_obj(handle);
  return r;
}

int RGWRados::prepare_update_index(RGWObjState *state, rgw_bucket& bucket,
				   RGWModifyOp op, rgw_obj& oid, string& tag)
{
  if (bucket_is_system(bucket))
    return 0;

  int ret = data_log->add_entry(oid.bucket);
  if (ret < 0) {
    lderr(cct) << "ERROR: failed writing data log" << dendl;
    return ret;
  }

  if (state && state->obj_tag.length()) {
    int len = state->obj_tag.length();
    char buf[len + 1];
    memcpy(buf, state->obj_tag.c_str(), len);
    buf[len] = '\0';
    tag = buf;
  } else {
    if (tag.empty()) {
      append_rand_alpha(cct, tag, tag, 32);
    }
  }
  ret = cls_obj_prepare_op(bucket, op, tag,
			   oid.object);

  return ret;
}

int RGWRados::complete_update_index(rgw_bucket& bucket, string& oid,
				    string& tag,
				    const boost::uuids::uuid& volid,
				    uint64_t size,
				    ceph::real_time&& ut, string& etag,
				    string& content_type, bufferlist *acl_bl,
				    RGWObjCategory category,
				    list<string> *remove_objs)
{
  if (bucket_is_system(bucket))
    return 0;

  RGWObjEnt ent;
  ent.name = oid;
  ent.size = size;
  ent.mtime = ut;
  ent.etag = etag;
  ACLOwner owner;
  if (acl_bl && acl_bl->length()) {
    int ret = decode_policy(*acl_bl, &owner);
    if (ret < 0) {
      ldout(cct, 0) << "WARNING: could not decode policy ret=" << ret << dendl;
    }
  }
  ent.owner = owner.get_id();
  ent.owner_display_name = owner.get_display_name();
  ent.content_type = content_type;

  int ret = cls_obj_complete_add(bucket, tag, volid, ent,
				 category, remove_objs);

  return ret;
}


int RGWRados::get_obj(void *ctx, RGWObjVersionTracker *objv_tracker,
		      void **handle, rgw_obj& obj,
		      bufferlist& bl, off_t ofs, off_t end)
{
  rgw_bucket bucket;
  std::string oid;
  rgw_obj read_obj = obj;
  uint64_t read_ofs = ofs;
  uint64_t len, read_len;
  RGWRadosCtx *rctx = static_cast<RGWRadosCtx *>(ctx);
  RGWRadosCtx *new_ctx = NULL;
  bool reading_from_head = true;
  GetObjState *state = *(GetObjState **)handle;
  RGWObjState *astate = NULL;
  ObjectOperation op(state->vol->op());

  bool merge_bl = false;
  bufferlist *pbl = &bl;
  bufferlist read_bl;

  get_obj_bucket_and_oid(obj, bucket, oid);

  if (!rctx) {
    new_ctx = new RGWRadosCtx(this);
    rctx = new_ctx;
  }

  int r = get_obj_state(rctx, obj, &astate, NULL);
  if (r < 0)
    goto done_ret;

  if (end < 0)
    len = 0;
  else
    len = end - ofs + 1;

  if (astate->has_manifest && astate->manifest.has_tail()) {
    /* now get the relevant object part */
    RGWObjManifest::obj_iterator iter = astate->manifest.obj_find(ofs);

    uint64_t stripe_ofs = iter.get_stripe_ofs();
    read_obj = iter.get_location();
    len = min(len, iter.get_stripe_size() - (ofs - stripe_ofs));
    read_ofs = iter.location_ofs() + (ofs - stripe_ofs);
    reading_from_head = (read_obj == obj);

    if (!reading_from_head) {
      get_obj_bucket_and_oid(read_obj, bucket, oid);
    }
  }

  if (len > max_chunk_size)
    len = max_chunk_size;


  read_len = len;

  if (reading_from_head) {
    /* only when reading from the head object do we need to do the
       atomic test */
    r = append_atomic_test(rctx, read_obj, op, &astate);
    if (r < 0)
      goto done_ret;

    if (astate && astate->prefetch_data) {
      if (!ofs && astate->data.length() >= len) {
	bl = astate->data;
	goto done;
      }

      if (ofs < astate->data.length()) {
	unsigned copy_len = min((uint64_t)astate->data.length() - ofs, len);
	astate->data.copy(ofs, copy_len, bl);
	read_len -= copy_len;
	read_ofs += copy_len;
	if (!read_len)
	  goto done;

	merge_bl = true;
	pbl = &read_bl;
      }
    }
  }

  if (objv_tracker) {
    objv_tracker->prepare_op_for_read(op);
  }


  ldout(cct, 20) << "rados->read oid-ofs=" << ofs << " read_ofs="
		 << read_ofs << " read_len=" << read_len << dendl;
  op->read(read_ofs, read_len, pbl, NULL);

  r = rc.objecter->mutate(oid, state->vol, op);
  ldout(cct, 20) << "rados->read r=" << r << " bl.length=" << bl.length()
		 << dendl;

  if (merge_bl)
    bl.append(read_bl);

done:
  if (bl.length() > 0) {
    r = bl.length();
  }
  if (r < 0 || !len || ((off_t)(ofs + len - 1) == end)) {
    finish_get_obj(handle);
  }

done_ret:
  delete new_ctx;

  return r;
}

struct get_obj_data;

struct get_obj_io {
  off_t len;
  bufferlist bl;
};

struct get_obj_data : public RefCountedObject {
  struct get_obj_aio_data : public rados::CB_Waiter {
    struct get_obj_data *op_data;
    off_t ofs;
    off_t len;

    get_obj_aio_data(get_obj_data* _op_data, off_t _ofs, off_t _len)
      : op_data(_op_data), ofs(_ofs), len(_len) {}
    get_obj_aio_data(const get_obj_aio_data&) = delete;
    get_obj_aio_data(get_obj_aio_data&&) = delete;

    void work(void) {
      list<bufferlist> bl_list;
      list<bufferlist>::iterator iter;

      ldout(op_data->cct, 20) << "get_obj_aio_completion_cb: io completion ofs="
			    << ofs << " len=" << len << dendl;
      op_data->throttle.put(len);

      get_obj_data::unique_lock dl(op_data->data_lock, std::defer_lock);

      if (op_data->is_cancelled())
	goto done;

      dl.lock();

      r = op_data->get_complete_ios(ofs, bl_list);
      if (r < 0) {
	goto done_unlock;
      }

      op_data->read_list.splice(op_data->read_list.end(), bl_list);

    done_unlock:
      dl.unlock();
    done:
      op_data->put();
      return;
    }
  };

  CephContext *cct;
  RGWRados *rados;
  void *ctx;
  VolumeRef io_vol;
  map<off_t, get_obj_io> io_map;
  uint64_t total_read;
  std::mutex lock;
  std::mutex data_lock;
  typedef std::lock_guard<std::mutex> lock_guard;
  typedef std::unique_lock<std::mutex> unique_lock;
  map<off_t, get_obj_aio_data> aio_data;
  RGWGetDataCB *client_cb;
  std::atomic<bool> cancelled;
  std::atomic<int> err_code;
  Throttle throttle;
  list<bufferlist> read_list;

  get_obj_data(CephContext *_cct)
    : cct(_cct),
      rados(NULL), ctx(NULL),
      total_read(0), client_cb(NULL),
      throttle(_cct, cct->_conf->rgw_get_obj_window_size) {}
  virtual ~get_obj_data() { }
  void set_cancelled(int r) {
    cancelled = true;
    err_code = r;
  }

  bool is_cancelled() {
    return cancelled;
  }

  int get_err_code() {
    return err_code;
  }

  int wait_next_io(bool *done) {
    unique_lock l(lock);
    auto iter = aio_data.begin();
    if (iter == aio_data.end()) {
      *done = true;
      l.unlock();
      return 0;
    }
    off_t cur_ofs = iter->first;
    auto& c = iter->second;
    l.unlock();

    int r = c.wait();

    l.lock();
    aio_data.erase(cur_ofs);

    if (aio_data.empty()) {
      *done = true;
    }
    l.unlock();

    return r;
  }

  void add_io(off_t ofs, off_t len, bufferlist **pbl,
	      op_callback& cb) {
    lock_guard l(lock);

    get_obj_io& io = io_map[ofs];
    *pbl = &io.bl;

    auto p = aio_data.emplace(std::piecewise_construct,
			      std::forward_as_tuple(ofs),
			      std::forward_as_tuple(this, ofs, len));


    cb = std::ref(p.first->second);

    /* we have a reference per IO, plus one reference for the calling function.
     * reference is dropped for each callback, plus when we're done iterating
     * over the parts */
    get();
  }

  void cancel_io(off_t ofs) {
    ldout(cct, 20) << "get_obj_data::cancel_io() ofs=" << ofs << dendl;
    unique_lock l(lock);
    auto iter = aio_data.find(ofs);
    if (iter != aio_data.end()) {
      // Implement properly when we have more coherent cancellation in
      // Objecter.
      abort();
      //c->release();
      //completion_map.erase(ofs);
      //io_map.erase(ofs);
    }
    l.unlock();
  }

  void cancel_all_io() {
    ldout(cct, 20) << "get_obj_data::cancel_all_io()" << dendl;
    lock_guard l(lock);
    abort();
    // for (auto& c : aio_data) {
    // }
  }

  int get_complete_ios(off_t ofs, list<bufferlist>& bl_list) {
    lock_guard l(lock);

    map<off_t, get_obj_io>::iterator liter = io_map.begin();

    if (liter == io_map.end() ||
	liter->first != ofs) {
      return 0;
    }

    auto aiter = aio_data.find(ofs);
    if (aiter == aio_data.end()) {
    /* completion map does not hold this io, it was cancelled */
      return 0;
    }

    get_obj_aio_data& completion = aiter->second;
    int r = completion.wait();
    if (r < 0)
      return r;

    for (; aiter != aio_data.end(); ++aiter) {
      auto& c = aiter->second;
      if (!c.complete()) {
	/* reached a request that is not yet complete, stop */
	break;
      }

      r = c.wait();
      if (r < 0) {
	set_cancelled(r); /* mark it as cancelled, so that we don't continue processing next operations */
	return r;
      }

      total_read += r;

      auto old_liter = liter++;
      bl_list.push_back(old_liter->second.bl);
      io_map.erase(old_liter);
    }

    return 0;
  }
};

static int _get_obj_iterate_cb(rgw_obj& oid, off_t obj_ofs,
			       off_t read_ofs, off_t len, bool is_head_obj,
			       RGWObjState *astate, void *arg)
{
  struct get_obj_data *d = (struct get_obj_data *)arg;

  return d->rados->get_obj_iterate_cb(d->ctx, astate, oid, obj_ofs, read_ofs,
				      len, is_head_obj, arg);
}


int RGWRados::flush_read_list(struct get_obj_data *d)
{
  get_obj_data::unique_lock dl(d->data_lock);
  list<bufferlist> l;
  l.swap(d->read_list);
  d->get();
  d->read_list.clear();

  dl.unlock();

  int r = 0;

  list<bufferlist>::iterator iter;
  for (iter = l.begin(); iter != l.end(); ++iter) {
    bufferlist& bl = *iter;
    wait_ref cb(std::make_unique<rados::CB_Waiter>());
    r = d->client_cb->handle_data(bl, 0, bl.length(), cb);
    if (r < 0) {
      dout(0) << "ERROR: flush_read_list(): d->client_c->handle_data() "
	      << "returned " << r << dendl;
      break;
    }
  }

  dl.lock();
  d->put();
  if (r < 0) {
    d->set_cancelled(r);
  }
  dl.unlock();
  return r;
}

int RGWRados::get_obj_iterate_cb(void *ctx, RGWObjState *astate,
				 rgw_obj& obj,
				 off_t obj_ofs,
				 off_t read_ofs, off_t len,
				 bool is_head_obj, void *arg)
{
  RGWRadosCtx *rctx = static_cast<RGWRadosCtx *>(ctx);
  struct get_obj_data *d = (struct get_obj_data *)arg;
  VolumeRef io_vol(d->io_vol);
  ObjectOperation op(io_vol->op());
  string oid;
  rgw_bucket bucket;
  bufferlist *pbl;
  rados::op_callback c;

  int r;

  if (is_head_obj) {
    /* only when reading from the head object do we need to do the
       atomic test */
    r = append_atomic_test(rctx, obj, op, &astate);
    if (r < 0)
      return r;

    if (astate &&
	obj_ofs < astate->data.length()) {
      unsigned chunk_len = min((uint64_t)astate->data.length() - obj_ofs,
			       (uint64_t)len);

      get_obj_data::unique_lock dl(d->data_lock);
      wait_ref cb(std::make_unique<rados::CB_Waiter>());
      r = d->client_cb->handle_data(astate->data, obj_ofs, chunk_len, cb);
      dl.unlock();
      if (r < 0)
	return r;

      dl.lock();
      d->total_read += chunk_len;
      dl.unlock();

      len -= chunk_len;
      read_ofs += chunk_len;
      obj_ofs += chunk_len;
      if (!len)
	  return 0;
    }
  }

  r = flush_read_list(d);
  if (r < 0)
    return r;

  get_obj_bucket_and_oid(obj, bucket, oid);

  d->throttle.get(len);
  if (d->is_cancelled()) {
    return d->get_err_code();
  }

  /* add io after we check that we're not cancelled, otherwise we're
   * going to have trouble cleaning up
   */
  d->add_io(obj_ofs, len, &pbl, c);

  ldout(cct, 20) << "rados->get_obj_iterate_cb oid=" << oid << " oid-ofs="
		 << obj_ofs << " read_ofs=" << read_ofs << " len=" << len
		 << dendl;
  op->read(read_ofs, len, pbl, NULL);

  r = rc.objecter->read(oid, io_vol, op, std::move(c));
  ldout(cct, 20) << "rados->aio_operate r=" << r << " bl.length="
		 << pbl->length() << dendl;
  if (r < 0)
    goto done_err;

  return 0;

done_err:
  ldout(cct, 20) << "cancelling io r=" << r << " obj_ofs=" << obj_ofs << dendl;
  d->set_cancelled(r);
  d->cancel_io(obj_ofs);

  return r;
}

int RGWRados::get_obj_iterate(void *ctx, void **handle, rgw_obj& oid,
			      off_t ofs, off_t end,
			      RGWGetDataCB *cb)
{
  struct get_obj_data *data = new get_obj_data(cct);
  bool done = false;

  GetObjState *state = *(GetObjState **)handle;

  data->rados = this;
  data->ctx = ctx;
  data->io_vol = state->vol;
  data->client_cb = cb;

  int r = iterate_obj(ctx, oid, ofs, end, cct->_conf->rgw_get_obj_max_req_size,
		      _get_obj_iterate_cb, (void *)data);
  if (r < 0) {
    data->cancel_all_io();
    goto done;
  }

  while (!done) {
    r = data->wait_next_io(&done);
    if (r < 0) {
      dout(10) << "get_obj_iterate() r=" << r << ", canceling all io" << dendl;
      data->cancel_all_io();
      break;
    }
    r = flush_read_list(data);
    if (r < 0) {
      dout(10) << "get_obj_iterate() r=" << r << ", canceling all io" << dendl;
      data->cancel_all_io();
      break;
    }
  }

done:
  data->put();
  return r;
}

void RGWRados::finish_get_obj(void **handle)
{
  if (*handle) {
    GetObjState *state = *(GetObjState **)handle;
    delete state;
    *handle = NULL;
  }
}

int RGWRados::iterate_obj(void *ctx, rgw_obj& oid,
			  off_t ofs, off_t end,
			  uint64_t max_chunk_size,
			  int (*iterate_obj_cb)(rgw_obj&, off_t, off_t, off_t,
						bool, RGWObjState *, void *),
			  void *arg)
{
  rgw_bucket bucket;
  rgw_obj read_obj = oid;
  uint64_t read_ofs = ofs;
  uint64_t len;
  RGWRadosCtx *rctx = static_cast<RGWRadosCtx *>(ctx);
  RGWRadosCtx *new_ctx = NULL;
  bool reading_from_head = true;
  RGWObjState *astate = NULL;

  if (!rctx) {
    new_ctx = new RGWRadosCtx(this);
    rctx = new_ctx;
  }

  int r = get_obj_state(rctx, oid, &astate, NULL);
  if (r < 0)
    goto done_err;

  if (end < 0)
    len = 0;
  else
    len = end - ofs + 1;

  if (astate->has_manifest) {
    /* now get the relevant object stripe */
    RGWObjManifest::obj_iterator iter = astate->manifest.obj_find(ofs);

    RGWObjManifest::obj_iterator obj_end = astate->manifest.obj_end();

    for (; iter != obj_end && ofs <= end; ++iter) {
      off_t stripe_ofs = iter.get_stripe_ofs();
      off_t next_stripe_ofs = stripe_ofs + iter.get_stripe_size();

      while (ofs < next_stripe_ofs && ofs <= end) {
	read_obj = iter.get_location();
	uint64_t read_len = min(len, iter.get_stripe_size() - (ofs - stripe_ofs));
	read_ofs = iter.location_ofs() + (ofs - stripe_ofs);

	if (read_len > max_chunk_size) {
	  read_len = max_chunk_size;
	}

	reading_from_head = (read_obj == oid);
	r = iterate_obj_cb(read_obj, ofs, read_ofs, read_len, reading_from_head, astate, arg);
	if (r < 0)
	  goto done_err;

	len -= read_len;
	ofs += read_len;
      }
    }
  } else {
    while (ofs <= end) {
      uint64_t read_len = min(len, max_chunk_size);

      r = iterate_obj_cb(oid, ofs, ofs, read_len, reading_from_head, astate, arg);
      if (r < 0)
	goto done_err;

      len -= read_len;
      ofs += read_len;
    }
  }

  return 0;

done_err:
  delete new_ctx;
  return r;
}

/* a simple object read */
int RGWRados::read(void *ctx, rgw_obj& oid, off_t ofs, size_t size, bufferlist& bl)
{
  rgw_rados_ref ref;
  rgw_bucket bucket;
  int r = get_obj_ref(oid, &ref, &bucket);
  if (r < 0) {
    return r;
  }
  RGWRadosCtx *rctx = static_cast<RGWRadosCtx *>(ctx);
  RGWObjState *astate = NULL;

  ObjectOperation op(ref.vol->op());

  r = append_atomic_test(rctx, oid, op, &astate);
  if (r < 0)
    return r;

  op->read(ofs, size, &bl, NULL);

  return rc.objecter->read(ref.oid, ref.vol, op);
}

int RGWRados::obj_stat(void *ctx, rgw_obj& oid, uint64_t *psize,
		       time_t *pmtime,
		       map<string, bufferlist> *attrs, bufferlist *first_chunk,
		       RGWObjVersionTracker *objv_tracker)
{
  rgw_rados_ref ref;
  rgw_bucket bucket;
  int r = get_obj_ref(oid, &ref, &bucket);
  if (r < 0) {
    return r;
  }

  map<string, bufferlist> unfiltered_attrset;
  uint64_t size = 0;
  ceph::real_time mtime;

  ObjectOperation op(ref.vol->op());
  if (objv_tracker) {
    objv_tracker->prepare_op_for_read(op);
  }
  op->getxattrs(unfiltered_attrset, NULL);
  op->stat(&size, &mtime, NULL);
  if (first_chunk) {
    op->read(0, cct->_conf->rgw_max_chunk_size, first_chunk, NULL);
  }
  r = rc.objecter->read(ref.oid, ref.vol, op);

  if (r < 0)
    return r;

  map<string, bufferlist> attrset;
  map<string, bufferlist>::iterator iter;
  string check_prefix = RGW_ATTR_PREFIX;
  for (iter = unfiltered_attrset.lower_bound(check_prefix);
       iter != unfiltered_attrset.end(); ++iter) {
    if (!str_startswith(iter->first, check_prefix))
      break;
    attrset[iter->first] = iter->second;
  }

  if (psize)
    *psize = size;
  if (pmtime)
    *pmtime = ceph::real_clock::to_time_t(mtime);
  if (attrs)
    *attrs = attrset;

  return 0;
}

int RGWRados::get_bucket_stats(rgw_bucket& bucket, uint64_t *bucket_ver, uint64_t *master_ver, map<RGWObjCategory, RGWStorageStats>& stats,
			       string *max_marker)
{
  rgw_bucket_dir_header header;
  int r = cls_bucket_head(bucket, header);
  if (r < 0)
    return r;

  stats.clear();

  translate_raw_stats(header, stats);

  *bucket_ver = header.ver;
  *master_ver = header.master_ver;

  if (max_marker)
    *max_marker = header.max_marker;

  return 0;
}

class RGWGetBucketStatsContext : public RGWGetDirHeader_CB {
  RGWGetBucketStats_CB *cb;

public:
  RGWGetBucketStatsContext(RGWGetBucketStats_CB *_cb) : cb(_cb) {}
  void handle_response(int r, rgw_bucket_dir_header& header) {
    map<RGWObjCategory, RGWStorageStats> stats;

    if (r >= 0) {
      translate_raw_stats(header, stats);
      cb->set_response(header.ver, header.master_ver, &stats, header.max_marker);
    }

    cb->handle_response(r);

    cb->put();
  }
};

int RGWRados::get_bucket_stats_async(rgw_bucket& bucket, RGWGetBucketStats_CB *ctx)
{
  RGWGetBucketStatsContext *get_ctx = new RGWGetBucketStatsContext(ctx);
  int r = cls_bucket_head_async(bucket, get_ctx);
  if (r < 0) {
    ctx->put();
    delete get_ctx;
    return r;
  }

  return 0;
}

class RGWGetUserStatsContext : public RGWGetUserHeader_CB {
  RGWGetUserStats_CB *cb;

public:
  RGWGetUserStatsContext(RGWGetUserStats_CB *_cb) : cb(_cb) {}
  void handle_response(int r, cls_user_header& header) {
    cls_user_stats& hs = header.stats;
    if (r >= 0) {
      RGWStorageStats stats;

      stats.num_kb = (hs.total_bytes + 1023) / 1024;
      stats.num_kb_rounded = (hs.total_bytes_rounded + 1023) / 1024;
      stats.num_objects = hs.total_entries;

      cb->set_response(stats);
    }

    cb->handle_response(r);

    cb->put();
  }
};

int RGWRados::get_user_stats(const string& user, RGWStorageStats& stats)
{
  cls_user_header header;
  int r = cls_user_get_header(user, &header);
  if (r < 0)
    return r;

  cls_user_stats& hs = header.stats;

  stats.num_kb = (hs.total_bytes + 1023) / 1024;
  stats.num_kb_rounded = (hs.total_bytes_rounded + 1023) / 1024;
  stats.num_objects = hs.total_entries;

  return 0;
}

int RGWRados::get_user_stats_async(const string& user, RGWGetUserStats_CB *ctx)
{
  RGWGetUserStatsContext *get_ctx = new RGWGetUserStatsContext(ctx);
  int r = cls_user_get_header_async(user, get_ctx);
  if (r < 0) {
    ctx->put();
    delete get_ctx;
    return r;
  }

  return 0;
}

void RGWRados::get_bucket_instance_entry(rgw_bucket& bucket, string& entry)
{
  entry = bucket.name + ":" + bucket.bucket_id;
}

void RGWRados::get_bucket_meta_oid(rgw_bucket& bucket, string& oid)
{
  string entry;
  get_bucket_instance_entry(bucket, entry);
  oid = RGW_BUCKET_INSTANCE_MD_PREFIX + entry;
}

void RGWRados::get_bucket_instance_obj(rgw_bucket& bucket, rgw_obj& obj)
{
  if (!bucket.obj.empty()) {
    obj.init(zone.domain_root, bucket.obj);
  } else {
    string oid;
    get_bucket_meta_oid(bucket, oid);
    obj.init(zone.domain_root, oid);
  }
}

int RGWRados::get_bucket_instance_info(void *ctx, const string& meta_key, RGWBucketInfo& info,
				       time_t *pmtime, map<string, bufferlist> *pattrs)
{
  int pos = meta_key.find(':');
  if (pos < 0) {
    return -EINVAL;
  }
  string oid = RGW_BUCKET_INSTANCE_MD_PREFIX + meta_key;

  return get_bucket_instance_from_oid(ctx, oid, info, pmtime, pattrs);
}

int RGWRados::get_bucket_instance_info(void *ctx, rgw_bucket& bucket,
				       RGWBucketInfo& info,
				       time_t *pmtime,
				       map<string, bufferlist> *pattrs)
{
  string oid;
  if (!bucket.obj.empty()) {
    get_bucket_meta_oid(bucket, oid);
  } else {
    oid = bucket.obj;
  }

  return get_bucket_instance_from_oid(ctx, oid, info, pmtime, pattrs);
}

int RGWRados::get_bucket_instance_from_oid(void *ctx, string& oid, RGWBucketInfo& info,
					   time_t *pmtime, map<string, bufferlist> *pattrs)
{
  ldout(cct, 20) << "reading from " << zone.domain_root << ":" << oid << dendl;

  bufferlist epbl;

  int ret = rgw_get_system_obj(this, ctx, zone.domain_root, oid, epbl, &info.objv_tracker, pmtime, pattrs);
  if (ret < 0) {
    return ret;
  }

  bufferlist::iterator iter = epbl.begin();
  try {
    ::decode(info, iter);
  } catch (buffer::error& err) {
    ldout(cct, 0) << "ERROR: could not decode buffer info, caught buffer::error" << dendl;
    return -EIO;
  }
  info.bucket.obj = oid;
  return 0;
}

int RGWRados::get_bucket_entrypoint_info(void *ctx, const string& bucket_name,
					 RGWBucketEntryPoint& entry_point,
					 RGWObjVersionTracker *objv_tracker,
					 time_t *pmtime,
					 map<string, bufferlist> *pattrs)
{
  bufferlist bl;

  int ret = rgw_get_system_obj(this, ctx, zone.domain_root, bucket_name, bl, objv_tracker, pmtime, pattrs);
  if (ret < 0) {
    return ret;
  }

  bufferlist::iterator iter = bl.begin();
  try {
    ::decode(entry_point, iter);
  } catch (buffer::error& err) {
    ldout(cct, 0) << "ERROR: could not decode buffer info, caught buffer::error" << dendl;
    return -EIO;
  }
  return 0;
}

int RGWRados::convert_old_bucket_info(void *ctx, string& bucket_name)
{
  RGWBucketEntryPoint entry_point;
  time_t ep_mtime;
  RGWObjVersionTracker ot;
  map<string, bufferlist> attrs;
  RGWBucketInfo info;

  ldout(cct, 10) << "RGWRados::convert_old_bucket_info(): bucket=" << bucket_name << dendl;

  int ret = get_bucket_entrypoint_info(ctx, bucket_name, entry_point, &ot, &ep_mtime, &attrs);
  if (ret < 0) {
    ldout(cct, 0) << "ERROR: get_bucket_entrypont_info() returned " << ret << " bucket=" << bucket_name << dendl;
    return ret;
  }

  if (!entry_point.has_bucket_info) {
    /* already converted! */
    return 0;
  }

  info = entry_point.old_bucket_info;
  info.bucket.obj = bucket_name;
  info.ep_objv = ot.read_version;

  ot.generate_new_write_ver(cct);

  ret = put_linked_bucket_info(info, false, ep_mtime, &ot.write_version, &attrs, true);
  if (ret < 0) {
    ldout(cct, 0) << "ERROR: failed to put_linked_bucket_info(): " << ret << dendl;
  }

  return 0;
}

int RGWRados::get_bucket_info(void *ctx, const string& bucket_name, RGWBucketInfo& info,
			      time_t *pmtime, map<string, bufferlist> *pattrs)
{
  bufferlist bl;

  RGWBucketEntryPoint entry_point;
  time_t ep_mtime;
  RGWObjVersionTracker ot;
  int ret = get_bucket_entrypoint_info(ctx, bucket_name, entry_point, &ot, &ep_mtime, pattrs);
  if (ret < 0) {
    info.bucket.name = bucket_name; /* only init this field */
    return ret;
  }

  if (entry_point.has_bucket_info) {
    info = entry_point.old_bucket_info;
    info.bucket.obj = bucket_name;
    info.ep_objv = ot.read_version;
    ldout(cct, 20) << "rgw_get_bucket_info: old bucket info, bucket=" << info.bucket << " owner " << info.owner << dendl;
    return 0;
  }

  /* data is in the bucket instance object, we need to get attributes from there, clear everything
   * that we got
   */
  if (pattrs) {
    pattrs->clear();
  }

  ldout(cct, 20) << "rgw_get_bucket_info: bucket instance: " << entry_point.bucket << dendl;

  if (pattrs)
    pattrs->clear();

  /* read bucket instance info */

  string oid;
  get_bucket_meta_oid(entry_point.bucket, oid);

  ret = get_bucket_instance_from_oid(ctx, oid, info, pmtime, pattrs);
  info.ep_objv = ot.read_version;
  if (ret < 0) {
    info.bucket.name = bucket_name;
    return ret;
  }
  return 0;
}

int RGWRados::put_bucket_entrypoint_info(const string& bucket_name, RGWBucketEntryPoint& entry_point,
					 bool exclusive, RGWObjVersionTracker& objv_tracker, time_t mtime,
					 map<string, bufferlist> *pattrs)
{
  bufferlist epbl;
  ::encode(entry_point, epbl);
  return rgw_bucket_store_info(this, bucket_name, epbl, exclusive, pattrs, &objv_tracker, mtime);
}

int RGWRados::put_bucket_instance_info(RGWBucketInfo& info, bool exclusive,
			      time_t mtime, map<string, bufferlist> *pattrs)
{
  info.has_instance_obj = true;
  bufferlist bl;

  ::encode(info, bl);

  string key;
  get_bucket_instance_entry(info.bucket, key); /* when we go through meta api, we don't use oid directly */
  return rgw_bucket_instance_store_info(this, key, bl, exclusive, pattrs, &info.objv_tracker, mtime);
}

int RGWRados::put_linked_bucket_info(RGWBucketInfo& info, bool exclusive, time_t mtime, obj_version *pep_objv,
				     map<string, bufferlist> *pattrs, bool create_entry_point)
{
  bufferlist bl;

  bool create_head = !info.has_instance_obj || create_entry_point;

  int ret = put_bucket_instance_info(info, exclusive, mtime, pattrs);
  if (ret < 0) {
    return ret;
  }

  if (!create_head)
    return 0; /* done! */

  RGWBucketEntryPoint entry_point;
  entry_point.bucket = info.bucket;
  entry_point.owner = info.owner;
  entry_point.creation_time = info.creation_time;
  entry_point.linked = true;
  RGWObjVersionTracker ot;
  if (pep_objv && !pep_objv->tag.empty()) {
    ot.write_version = *pep_objv;
  } else {
    ot.generate_new_write_ver(cct);
    if (pep_objv) {
      *pep_objv = ot.write_version;
    }
  }
  ret = put_bucket_entrypoint_info(info.bucket.name, entry_point, exclusive, ot, mtime, NULL);
  if (ret < 0)
    return ret;

  return 0;
}

int RGWRados::omap_get_vals(rgw_obj& oid, bufferlist& header, const string& marker, uint64_t count, std::map<string, bufferlist>& m)
{
  rgw_rados_ref ref;
  rgw_bucket bucket;
  int r = get_obj_ref(oid, &ref, &bucket);
  if (r < 0) {
    return r;
  }

  r = rc.objecter->omap_get_vals(ref.oid, ref.vol, string(), marker, count, m);
  if (r < 0)
    return r;

  return 0;

}

int RGWRados::omap_get_all(rgw_obj& oid, bufferlist& header, std::map<string, bufferlist>& m)
{
  string start_after;

  return omap_get_vals(oid, header, start_after, (uint64_t)-1, m);
}

int RGWRados::omap_set(rgw_obj& oid, std::string& key, bufferlist& bl)
{
  rgw_rados_ref ref;
  rgw_bucket bucket;
  int r = get_obj_ref(oid, &ref, &bucket);
  if (r < 0) {
    return r;
  }
  ldout(cct, 15) << "omap_set bucket=" << bucket << " oid=" << ref.oid << " key=" << key << dendl;

  map<string, bufferlist> m;
  m[key] = bl;

  r = rc.objecter->omap_set(ref.oid, ref.vol, m);

  return r;
}

int RGWRados::omap_set(rgw_obj& oid, std::map<std::string, bufferlist>& m)
{
  rgw_rados_ref ref;
  rgw_bucket bucket;
  int r = get_obj_ref(oid, &ref, &bucket);
  if (r < 0) {
    return r;
  }

  r = rc.objecter->omap_set(ref.oid, ref.vol, m);

  return r;
}

int RGWRados::omap_del(rgw_obj& oid, const std::string& key)
{
  rgw_rados_ref ref;
  rgw_bucket bucket;
  int r = get_obj_ref(oid, &ref, &bucket);
  if (r < 0) {
    return r;
  }

  std::set<string> k;
  k.insert(key);

  r = rc.objecter->omap_rm_keys(ref.oid, ref.vol, k);
  return r;
}

int RGWRados::update_containers_stats(map<string, RGWBucketEnt>& m)
{
  map<string, RGWBucketEnt>::iterator iter;
  for (iter = m.begin(); iter != m.end(); ++iter) {
    RGWBucketEnt& ent = iter->second;
    rgw_bucket& bucket = ent.bucket;

    rgw_bucket_dir_header header;
    int r = cls_bucket_head(bucket, header);
    if (r < 0)
      return r;

    ent.count = 0;
    ent.size = 0;

    RGWObjCategory category = main_category;
    map<uint8_t, struct rgw_bucket_category_stats>::iterator iter = header.stats.find((uint8_t)category);
    if (iter != header.stats.end()) {
      struct rgw_bucket_category_stats& stats = iter->second;
      ent.count = stats.num_entries;
      ent.size = stats.total_size;
      ent.size_rounded = stats.total_size_rounded;
    }
  }

  return m.size();
}

int RGWRados::append_async(rgw_obj& oid, size_t size, bufferlist& bl)
{
  rgw_rados_ref ref;
  rgw_bucket bucket;
  int r = get_obj_ref(oid, &ref, &bucket);
  if (r < 0) {
    return r;
  }
  r = rc.objecter->append(ref.oid, ref.vol, size, bl, nullptr, nullptr);
  return r;
}

int RGWRados::vol_iterate_begin(rgw_bucket& bucket, RGWVolIterCtx& ctx)
{
  VolumeRef io_vol = ctx.v;

  int r = open_bucket_data_vol(bucket, io_vol);
  if (r < 0)
    return r;

  return 0;
}

int RGWRados::vol_iterate(RGWVolIterCtx& ctx, uint32_t num,
			  vector<RGWObjEnt>& objs,
			  bool *is_truncated, RGWAccessListFilter *filter)
{
  return objs.size();
}
struct RGWAccessListFilterPrefix : public RGWAccessListFilter {
  string prefix;

  RGWAccessListFilterPrefix(const string& _prefix) : prefix(_prefix) {}
  virtual bool filter(string& name, string& key) {
    return (prefix.compare(key.substr(0, prefix.size())) == 0);
  }
};

int RGWRados::list_raw_objects(rgw_bucket& vol, const string& prefix_filter,
			       int max, RGWListRawObjsCtx& ctx,
			       list<string>& oids, bool *is_truncated)
{
  RGWAccessListFilterPrefix filter(prefix_filter);

  if (!ctx.initialized) {
    int r = vol_iterate_begin(vol, ctx.iter_ctx);
    if (r < 0) {
      lderr(cct) << "failed to list objects vol_iterate_begin() returned r="
		 << r << dendl;
      return r;
    }
    ctx.initialized = true;
  }

  vector<RGWObjEnt> objs;
  int r = vol_iterate(ctx.iter_ctx, max, objs, is_truncated, &filter);
  if (r < 0) {
    lderr(cct) << "failed to list objects vol_iterate returned r=" << r
	       << dendl;
    return r;
  }

  vector<RGWObjEnt>::iterator iter;
  for (iter = objs.begin(); iter != objs.end(); ++iter) {
    oids.push_back(iter->name);
  }

  return oids.size();
}

int RGWRados::list_bi_log_entries(rgw_bucket& bucket, string& marker,
				  uint32_t max,
				  std::list<rgw_bi_log_entry>& result,
				  bool *truncated)
{
  result.clear();

  VolumeRef index_vol;
  string oid;
  int r = open_bucket_index(bucket, index_vol, oid);
  if (r < 0)
    return r;

  std::list<rgw_bi_log_entry> entries;
  int ret = cls_rgw_bi_log_list(rc.objecter, index_vol, oid, marker,
				max - result.size(), entries, truncated);
  if (ret < 0)
    return ret;

  std::list<rgw_bi_log_entry>::iterator iter;
  for (iter = entries.begin(); iter != entries.end(); ++iter) {
    result.push_back(*iter);
  }

  return 0;
}

int RGWRados::trim_bi_log_entries(rgw_bucket& bucket, string& start_marker,
				  string& end_marker)
{
  VolumeRef index_vol;
  string oid;
  int r = open_bucket_index(bucket, index_vol, oid);
  if (r < 0)
    return r;

  int ret = cls_rgw_bi_log_trim(rc.objecter, index_vol, oid, start_marker,
				end_marker);
  if (ret < 0)
    return ret;

  return 0;
}

int RGWRados::gc_operate(string& oid, ObjOpOwn op)
{
  return rc.objecter->mutate(oid, gc_vol, op);
}

int RGWRados::gc_aio_operate(string& oid, ObjOpOwn op)
{
  return rc.objecter->mutate(oid, gc_vol, op, ceph::real_clock::now(), nullptr,
			     nullptr);
}

int RGWRados::list_gc_objs(int *index, string& marker, uint32_t max,
			   bool expired_only,
			   std::list<cls_rgw_gc_obj_info>& result,
			   bool *truncated)
{
  return gc->list(index, marker, max, expired_only, result, truncated);
}

int RGWRados::process_gc()
{
  return gc->process();
}

int RGWRados::cls_rgw_init_index(RadosClient& rc, VolumeRef index_vol,
				 ObjOpOwn op, string& oid)
{
  bufferlist in;
  cls_rgw_bucket_init(op);
  int r = rc.objecter->mutate(oid, index_vol, op);
  return r;
}

int RGWRados::cls_obj_prepare_op(rgw_bucket& bucket, RGWModifyOp op,
				 string& tag, string& name)
{
  VolumeRef index_vol;
  string oid;

  int r = open_bucket_index(bucket, index_vol, oid);
  if (r < 0)
    return r;

  ObjectOperation o(index_vol->op());
  cls_rgw_bucket_prepare_op(o, op, tag, name,
			    zone_public_config.log_data);
  r = rc.objecter->mutate(oid, index_vol, o);
  return r;
}

int RGWRados::cls_obj_complete_op(rgw_bucket& bucket, RGWModifyOp op,
				  string& tag,
				  const boost::uuids::uuid& volid,
				  RGWObjEnt& ent, RGWObjCategory category,
				  list<string> *remove_objs)
{
  VolumeRef index_vol;
  string oid;

  int r = open_bucket_index(bucket, index_vol, oid);
  if (r < 0)
    return r;

  ObjectOperation o(index_vol->op());
  rgw_bucket_dir_entry_meta dir_meta;
  dir_meta.size = ent.size;
  dir_meta.mtime = ent.mtime;
  dir_meta.etag = ent.etag;
  dir_meta.owner = ent.owner;
  dir_meta.owner_display_name = ent.owner_display_name;
  dir_meta.content_type = ent.content_type;
  dir_meta.category = category;

  rgw_bucket_entry_ver ver;
  ver.vol = volid;
  cls_rgw_bucket_complete_op(o, op, tag, ver, ent.name, dir_meta, remove_objs,
			     zone_public_config.log_data);

  r = rc.objecter->mutate(oid, index_vol, o, ceph::real_clock::now(), nullptr,
			  nullptr);
  return r;
}

int RGWRados::cls_obj_complete_add(rgw_bucket& bucket, string& tag,
				   const boost::uuids::uuid& vol,
				   RGWObjEnt& ent, RGWObjCategory category,
				   list<string> *remove_objs)
{
  return cls_obj_complete_op(bucket, CLS_RGW_OP_ADD, tag, vol, ent,
			     category, remove_objs);
}

int RGWRados::cls_obj_complete_del(rgw_bucket& bucket, string& tag,
				   const boost::uuids::uuid& vol,
				   string& name)
{
  RGWObjEnt ent;
  ent.name = name;
  return cls_obj_complete_op(bucket, CLS_RGW_OP_DEL, tag, vol, ent,
			     RGW_OBJ_CATEGORY_NONE, NULL);
}

int RGWRados::cls_obj_complete_cancel(rgw_bucket& bucket, string& tag,
				      string& name)
{
  RGWObjEnt ent;
  ent.name = name;
  return cls_obj_complete_op(bucket, CLS_RGW_OP_ADD, tag,
			     boost::uuids::nil_uuid(), ent,
			     RGW_OBJ_CATEGORY_NONE, nullptr);
}

int RGWRados::cls_obj_set_bucket_tag_timeout(rgw_bucket& bucket,
					     uint64_t timeout)
{
  VolumeRef index_vol;
  string oid;

  int r = open_bucket_index(bucket, index_vol, oid);
  if (r < 0)
    return r;

  ObjectOperation o(index_vol->op());
  cls_rgw_bucket_set_tag_timeout(o, timeout);

  r = rc.objecter->mutate(oid, index_vol, o);

  return r;
}

int RGWRados::cls_bucket_list(rgw_bucket& bucket, string start, string prefix,
			      uint32_t num, map<string, RGWObjEnt>& m,
			      bool *is_truncated, string *last_entry,
			      bool (*force_check_filter)(const string&	name))
{
  ldout(cct, 10) << "cls_bucket_list " << bucket << " start " << start << " num " << num << dendl;

  VolumeRef index_vol;
  string oid;
  int r = open_bucket_index(bucket, index_vol, oid);
  if (r < 0)
    return r;

  struct rgw_bucket_dir dir;
  r = cls_rgw_list_op(rc.objecter, index_vol, oid, start, prefix, num, &dir,
		      is_truncated);
  if (r < 0)
    return r;

  map<string, struct rgw_bucket_dir_entry>::iterator miter;
  bufferlist updates;
  for (miter = dir.m.begin(); miter != dir.m.end(); ++miter) {
    RGWObjEnt e;
    rgw_bucket_dir_entry& dirent = miter->second;

    // fill it in with initial values; we may correct later
    e.name = dirent.name;
    e.size = dirent.meta.size;
    e.mtime = dirent.meta.mtime;
    e.etag = dirent.meta.etag;
    e.owner = dirent.meta.owner;
    e.owner_display_name = dirent.meta.owner_display_name;
    e.content_type = dirent.meta.content_type;
    e.tag = dirent.tag;

    /* oh, that shouldn't happen! */
    if (e.name.empty()) {
      ldout(cct, 0) << "WARNING: got empty dirent name, skipping" << dendl;
      continue;
    }

    bool force_check = force_check_filter && force_check_filter(dirent.name);

    if (!dirent.exists || !dirent.pending_map.empty() || force_check) {
      /* there are uncommitted ops. We need to check the current state,
       * and if the tags are old we need to do cleanup as well. */
      r = check_disk_state(index_vol, bucket, dirent, e, updates);
      if (r < 0) {
	if (r == -ENOENT)
	  continue;
	else
	  return r;
      }
    }
    m[e.name] = e;
    ldout(cct, 10) << "RGWRados::cls_bucket_list: got " << e.name << dendl;
  }

  if (dir.m.size()) {
    *last_entry = dir.m.rbegin()->first;
  }

  if (updates.length()) {
    ObjectOperation o(index_vol->op());
    cls_rgw_suggest_changes(o, updates);
    r = rc.objecter->mutate(oid, index_vol, o, ceph::real_clock::now(),
			    nullptr, nullptr);
  }
  return m.size();
}

int RGWRados::cls_obj_usage_log_add(const string& oid, rgw_usage_log_info& info)
{
  VolumeRef io_vol;

  int r = 0;
  const char *usage_log_vol = zone.usage_log_vol.name.c_str();
  io_vol = rc.lookup_volume(usage_log_vol);
  if (!io_vol) {
    rgw_bucket vol(usage_log_vol);
    r = create_vol(vol);
    if (r < 0)
      return r;

    // retry
    io_vol = rc.lookup_volume(usage_log_vol);
    if (!io_vol)
      r = -ENOENT;
  }
  if (r < 0)
    return r;

  ObjectOperation op(io_vol->op());
  cls_rgw_usage_log_add(op, info);

  r = rc.objecter->mutate(oid, io_vol, op);
  return r;
}

int RGWRados::cls_obj_usage_log_read(string& oid, string& user,
				     uint32_t max_entries,
				     string& read_iter, map<rgw_user_bucket,
				     rgw_usage_log_entry>& usage,
				     bool *is_truncated)
{
  VolumeRef io_vol;

  *is_truncated = false;

  int r = open_bucket_index_vol(zone.usage_log_vol, io_vol);
  if (r < 0)
    return r;

  r = cls_rgw_usage_log_read(rc.objecter, io_vol, oid, user, max_entries,
			     read_iter, usage, is_truncated);

  return r;
}

int RGWRados::cls_obj_usage_log_trim(string& oid, string& user)
{
  VolumeRef io_vol;

  int r = open_bucket_index_vol(zone.usage_log_vol, io_vol);
  if (r < 0)
    return r;

  ObjectOperation op(io_vol->op());
  cls_rgw_usage_log_trim(op, user);

  r = rc.objecter->mutate(oid, io_vol, op);
  return r;
}

int RGWRados::remove_objs_from_index(rgw_bucket& bucket,
				     list<string>& oid_list)
{
  VolumeRef index_vol;
  string dir_oid;

  int r = open_bucket_index(bucket, index_vol, dir_oid);
  if (r < 0)
    return r;

  bufferlist updates;

  list<string>::iterator iter;

  for (iter = oid_list.begin(); iter != oid_list.end(); ++iter) {
    string& oid = *iter;
    dout(2) << "RGWRados::remove_objs_from_index bucket=" << bucket
	    << " oid=" << oid << dendl;
    rgw_bucket_dir_entry entry;
    // ULLONG_MAX, needed to that objclass doesn't skip out request
    entry.name = oid;
    updates.append(CEPH_RGW_REMOVE);
    ::encode(entry, updates);
  }

  bufferlist out;

  ObjectOperation op(index_vol->op());
  op->call("rgw", "dir_suggest_changes", updates, &out);
  r = rc.objecter->mutate(dir_oid, index_vol, op);

  return r;
}

int RGWRados::check_disk_state(VolumeRef io_vol,
			       rgw_bucket& bucket,
			       rgw_bucket_dir_entry& list_state,
			       RGWObjEnt& object,
			       bufferlist& suggested_updates)
{
  rgw_obj obj;
  std::string oid;
  oid = list_state.name;
  obj.init(bucket, oid);
  get_obj_bucket_and_oid(obj, bucket, oid);

  RGWObjState *astate = NULL;
  RGWRadosCtx rctx(this);
  int r = get_obj_state(&rctx, obj, &astate, NULL);
  if (r < 0)
    return r;

  list_state.pending_map.clear(); // we don't need this and it inflates size
  if (!astate->exists) {
      /* object doesn't exist right now -- hopefully because it's
       * marked as !exists and got deleted */
    if (list_state.exists) {
      /* FIXME: what should happen now? Work out if there are any
       * non-bad ways this could happen (there probably are, but annoying
       * to handle!) */
    }
    // encode a suggested removal of that key
    list_state.ver.vol = io_vol->id;
    cls_rgw_encode_suggestion(CEPH_RGW_REMOVE, list_state, suggested_updates);
    return -ENOENT;
  }

  string etag;
  string content_type;
  ACLOwner owner;

  object.size = astate->size;
  object.mtime = ceph::real_clock::from_time_t(astate->mtime);

  map<string, bufferlist>::iterator iter = astate->attrset.find(RGW_ATTR_ETAG);
  if (iter != astate->attrset.end()) {
    etag = iter->second.c_str();
  }
  iter = astate->attrset.find(RGW_ATTR_CONTENT_TYPE);
  if (iter != astate->attrset.end()) {
    content_type = iter->second.c_str();
  }
  iter = astate->attrset.find(RGW_ATTR_ACL);
  if (iter != astate->attrset.end()) {
    r = decode_policy(iter->second, &owner);
    if (r < 0) {
      dout(0) << "WARNING: could not decode policy for object: " << oid << dendl;
    }
  }

  if (astate->has_manifest) {
    RGWObjManifest::obj_iterator miter;
    RGWObjManifest& manifest = astate->manifest;
    for (miter = manifest.obj_begin(); miter != manifest.obj_end(); ++miter) {
      rgw_obj loc = miter.get_location();
    }
  }

  object.etag = etag;
  object.content_type = content_type;
  object.owner = owner.get_id();
  object.owner_display_name = owner.get_display_name();

  // encode suggested updates
  list_state.ver.vol = io_vol->id;
  list_state.meta.size = object.size;
  list_state.meta.mtime = object.mtime;
  list_state.meta.category = main_category;
  list_state.meta.etag = etag;
  list_state.meta.content_type = content_type;
  if (astate->obj_tag.length() > 0)
    list_state.tag = astate->obj_tag.c_str();
  list_state.meta.owner = owner.get_id();
  list_state.meta.owner_display_name = owner.get_display_name();

  list_state.exists = true;
  cls_rgw_encode_suggestion(CEPH_RGW_UPDATE, list_state, suggested_updates);
  return 0;
}

int RGWRados::cls_bucket_head(rgw_bucket& bucket,
			      struct rgw_bucket_dir_header& header)
{
  VolumeRef index_vol;
  string oid;
  int r = open_bucket_index(bucket, index_vol, oid);
  if (r < 0)
    return r;

  r = cls_rgw_get_dir_header(rc.objecter, index_vol, oid, &header);
  if (r < 0)
    return r;

  return 0;
}

int RGWRados::cls_bucket_head_async(rgw_bucket& bucket,
				    RGWGetDirHeader_CB *ctx)
{
  VolumeRef index_vol;
  string oid;
  int r = open_bucket_index(bucket, index_vol, oid);
  if (r < 0)
    return r;

  r = cls_rgw_get_dir_header_async(rc.objecter, index_vol, oid, ctx);
  if (r < 0)
    return r;

  return 0;
}

int RGWRados::cls_user_get_header(const string& user_id,
				  cls_user_header *header)
{
  string buckets_obj_id;
  rgw_get_buckets_obj(user_id, buckets_obj_id);
  rgw_obj oid(zone.user_uid_vol, buckets_obj_id);

  rgw_rados_ref ref;
  rgw_bucket bucket;
  int r = get_obj_ref(oid, &ref, &bucket);
  if (r < 0) {
    return r;
  }

  ObjectOperation op(ref.vol->op());
  int retcode;
  ::cls_user_get_header(op, header, &retcode);
  r = rc.objecter->read(ref.oid, ref.vol, op);
  if (r < 0)
    return r;
  if (retcode < 0)
    return retcode;

  return 0;
}

int RGWRados::cls_user_get_header_async(const string& user_id, RGWGetUserHeader_CB *ctx)
{
  string buckets_obj_id;
  rgw_get_buckets_obj(user_id, buckets_obj_id);
  rgw_obj oid(zone.user_uid_vol, buckets_obj_id);

  rgw_rados_ref ref;
  rgw_bucket bucket;
  int r = get_obj_ref(oid, &ref, &bucket);
  if (r < 0) {
    return r;
  }

  r = ::cls_user_get_header_async(rc.objecter, ref.vol, ref.oid, ctx);
  if (r < 0)
    return r;

  return 0;
}

int RGWRados::cls_user_sync_bucket_stats(rgw_obj& user_obj, rgw_bucket& bucket)
{
  rgw_bucket_dir_header header;
  int r = cls_bucket_head(bucket, header);
  if (r < 0) {
    ldout(cct, 20) << "cls_bucket_header() returned " << r << dendl;
    return r;
  }

  cls_user_bucket_entry entry;

  bucket.convert(&entry.bucket);

  map<uint8_t, struct rgw_bucket_category_stats>::iterator iter = header.stats.begin();
  for (; iter != header.stats.end(); ++iter) {
    struct rgw_bucket_category_stats& header_stats = iter->second;
    entry.size += header_stats.total_size;
    entry.size_rounded += header_stats.total_size_rounded;
    entry.count += header_stats.num_entries;
  }

  list<cls_user_bucket_entry> entries;
  entries.push_back(entry);

  r = cls_user_update_buckets(user_obj, entries, false);
  if (r < 0) {
    ldout(cct, 20) << "cls_user_update_buckets() returned " << r << dendl;
    return r;
  }

  return 0;
}

int RGWRados::update_user_bucket_stats(const string& user_id, rgw_bucket& bucket, RGWStorageStats& stats)
{
  cls_user_bucket_entry entry;

  entry.size = stats.num_kb * 1024;
  entry.size_rounded = stats.num_kb_rounded * 1024;
  entry.count += stats.num_objects;

  list<cls_user_bucket_entry> entries;
  entries.push_back(entry);

  string buckets_obj_id;
  rgw_get_buckets_obj(user_id, buckets_obj_id);
  rgw_obj oid(zone.user_uid_vol, buckets_obj_id);

  int r = cls_user_update_buckets(oid, entries, false);
  if (r < 0) {
    ldout(cct, 20) << "cls_user_update_buckets() returned " << r << dendl;
    return r;
  }

  return 0;
}

int RGWRados::cls_user_list_buckets(rgw_obj& oid,
				    const string& in_marker, int max_entries,
				    list<cls_user_bucket_entry>& entries,
				    string *out_marker, bool *truncated)
{
  rgw_rados_ref ref;
  rgw_bucket bucket;
  int r = get_obj_ref(oid, &ref, &bucket);
  if (r < 0) {
    return r;
  }

  ObjectOperation op(ref.vol->op());
  int retcode;

  cls_user_bucket_list(op, in_marker, max_entries, entries, out_marker,
		       truncated, &retcode);
  r = rc.objecter->read(ref.oid, ref.vol, op);
  if (r < 0)
    return r;
  if (retcode < 0)
    return retcode;

  return 0;
}

int RGWRados::cls_user_update_buckets(rgw_obj& oid, list<cls_user_bucket_entry>& entries, bool add)
{
  rgw_rados_ref ref;
  rgw_bucket bucket;
  int r = get_obj_ref(oid, &ref, &bucket);
  if (r < 0) {
    return r;
  }

  ObjectOperation op(ref.vol->op());
  cls_user_set_buckets(op, entries, add);
  r = rc.objecter->mutate(ref.oid, ref.vol, op);
  if (r < 0)
    return r;

  return 0;
}

int RGWRados::complete_sync_user_stats(const string& user_id)
{
  string buckets_obj_id;
  rgw_get_buckets_obj(user_id, buckets_obj_id);
  rgw_obj oid(zone.user_uid_vol, buckets_obj_id);
  return cls_user_complete_stats_sync(oid);
}

int RGWRados::cls_user_complete_stats_sync(rgw_obj& oid)
{
  rgw_rados_ref ref;
  rgw_bucket bucket;
  int r = get_obj_ref(oid, &ref, &bucket);
  if (r < 0) {
    return r;
  }

  ObjectOperation op(ref.vol->op());
  ::cls_user_complete_stats_sync(op);
  r = rc.objecter->read(ref.oid, ref.vol, op);
  if (r < 0)
    return r;

  return 0;
}

int RGWRados::cls_user_add_bucket(rgw_obj& oid, const cls_user_bucket_entry& entry)
{
  list<cls_user_bucket_entry> l;
  l.push_back(entry);

  return cls_user_update_buckets(oid, l, true);
}

int RGWRados::cls_user_remove_bucket(rgw_obj& oid, const cls_user_bucket& bucket)
{
  rgw_bucket b;
  rgw_rados_ref ref;
  int r = get_obj_ref(oid, &ref, &b);
  if (r < 0) {
    return r;
  }

  ObjectOperation op(ref.vol->op());
  ::cls_user_remove_bucket(op, bucket);
  r = rc.objecter->mutate(ref.oid, ref.vol, op);
  if (r < 0)
    return r;

  return 0;
}

int RGWRados::check_quota(const string& bucket_owner, rgw_bucket& bucket,
			  RGWQuotaInfo& user_quota, RGWQuotaInfo& bucket_quota, uint64_t obj_size)
{
  return quota_handler->check_quota(bucket_owner, bucket, user_quota, bucket_quota, 1, obj_size);
}

class IntentLogNameFilter : public RGWAccessListFilter
{
  string prefix;
  bool filter_exact_date;
public:
  IntentLogNameFilter(const char *date, struct tm *tm) : prefix(date) {
    filter_exact_date = !(tm->tm_hour || tm->tm_min || tm->tm_sec); /* if time was specified and is not 00:00:00
								       we should look at objects from that date */
  }
  bool filter(string& name, string& key) {
    if (filter_exact_date)
      return name.compare(prefix) < 0;
    else
      return name.compare(0, prefix.size(), prefix) <= 0;
  }
};

enum IntentFlags { // bitmask
  I_DEL_OBJ = 1,
  I_DEL_DIR = 2,
};


int RGWRados::remove_temp_objects(string date, string time)
{
  struct tm tm;

  string format = "%Y-%m-%d";
  string datetime = date;
  if (datetime.size() != 10) {
    cerr << "bad date format" << std::endl;
    return -EINVAL;
  }

  if (!time.empty()) {
    if (time.size() != 5 && time.size() != 8) {
      cerr << "bad time format" << std::endl;
      return -EINVAL;
    }
    format.append(" %H:%M:%S");
    datetime.append(time.c_str());
  }
  memset(&tm, 0, sizeof(tm));
  const char *s = strptime(datetime.c_str(), format.c_str(), &tm);
  if (s && *s) {
    cerr << "failed to parse date/time" << std::endl;
    return -EINVAL;
  }
  time_t epoch = mktime(&tm);

  vector<RGWObjEnt> objs;

  int max = 1000;
  bool is_truncated;
  IntentLogNameFilter filter(date.c_str(), &tm);
  RGWVolIterCtx iter_ctx;
  int r = vol_iterate_begin(zone.intent_log_vol, iter_ctx);
  if (r < 0) {
    cerr << "failed to list objects" << std::endl;
    return r;
  }
  do {
    objs.clear();
    r = vol_iterate(iter_ctx, max, objs, &is_truncated, &filter);
    if (r == -ENOENT)
      break;
    if (r < 0) {
      cerr << "failed to list objects" << std::endl;
    }
    vector<RGWObjEnt>::iterator iter;
    for (iter = objs.begin(); iter != objs.end(); ++iter) {
      process_intent_log(zone.intent_log_vol, (*iter).name, epoch, I_DEL_OBJ | I_DEL_DIR, true);
    }
  } while (is_truncated);

  return 0;
}

int RGWRados::process_intent_log(rgw_bucket& bucket, string& oid,
				 time_t epoch, int flags, bool purge)
{
  cout << "processing intent log " << oid << std::endl;
  rgw_obj obj(bucket, oid);

  unsigned chunk = 1024 * 1024;
  off_t pos = 0;
  bool eof = false;
  bool complete = true;
  int ret = 0;
  int r;

  bufferlist bl;
  bufferlist::iterator iter;
  off_t off;

  string no_owner;

  while (!eof || !iter.end()) {
    off = iter.get_off();
    if (!eof && (bl.length() - off) < chunk / 2) {
      bufferlist more;
      r = read(NULL, obj, pos, chunk, more);
      if (r < 0) {
	cerr << "error while reading from " <<	bucket << ":" << oid
	   << " " << cpp_strerror(-r) << std::endl;
	return -r;
      }
      eof = (more.length() < (off_t)chunk);
      pos += more.length();
      bufferlist old;
      old.substr_of(bl, off, bl.length() - off);
      bl.clear();
      bl.claim(old);
      bl.claim_append(more);
      iter = bl.begin();
    }

    struct rgw_intent_log_entry entry;
    try {
      ::decode(entry, iter);
    } catch (buffer::error& err) {
      cerr << "failed to decode intent log entry in " << bucket << ":"
	   << oid << std::endl;
      cerr << "skipping log" << std::endl; // no use to continue
      ret = -EIO;
      complete = false;
      break;
    }
    if (ceph::real_clock::to_time_t(entry.op_time) > epoch) {
      cerr << "skipping entry for oid=" << obj << " entry.op_time="
	   << entry.op_time << " requested epoch=" << epoch << std::endl;
      cerr << "skipping log" << std::endl; // no use to continue
      complete = false;
      break;
    }
    switch (entry.intent) {
    case DEL_OBJ:
      if (!(flags & I_DEL_OBJ)) {
	complete = false;
	break;
      }
      r = delete_obj(NULL, no_owner, entry.oid);
      if (r < 0 && r != -ENOENT) {
	cerr << "failed to remove oid: " << entry.oid << std::endl;
	complete = false;
      }
      break;
    case DEL_DIR:
      if (!(flags & I_DEL_DIR)) {
	complete = false;
	break;
      } else {
	VolumeRef index_vol;
	string oid;
	int r = open_bucket_index(entry.oid.bucket, index_vol, oid);
	if (r < 0)
	  return r;
	ObjectOperation op(index_vol->op());
	op->remove();
	oid.append(entry.oid.bucket.marker);
	r = rc.objecter->mutate(oid, index_vol, op, ceph::real_clock::now(),
				nullptr, nullptr);
	if (r < 0 && r != -ENOENT) {
	  cerr << "failed to remove bucket: " << entry.oid.bucket << std::endl;
	  complete = false;
	}
      }
      break;
    default:
      complete = false;
    }
  }

  if (complete) {
    rgw_obj obj(bucket, oid);
    cout << "completed intent log: " << oid << (purge ? ", purging it" : "") << std::endl;
    if (purge) {
      r = delete_system_obj(NULL, obj);
      if (r < 0)
	cerr << "failed to remove oid: " << obj << std::endl;
    }
  }

  return ret;
}


void RGWStateLog::oid_str(int shard, string& oid) {
  oid = RGW_STATELOG_OBJ_PREFIX + module_name + ".";
  char buf[16];
  snprintf(buf, sizeof(buf), "%d", shard);
  oid += buf;
}

int RGWStateLog::get_shard_num(const string& object) {
  uint32_t val = ceph_str_hash_linux(object.c_str(), object.length());
  return val % num_shards;
}

string RGWStateLog::get_oid(const string& object) {
  int shard = get_shard_num(object);
  string oid;
  oid_str(shard, oid);
  return oid;
}

int RGWStateLog::open_vol(VolumeRef& vol) {
  string vol_name;
  store->get_log_vol_name(vol_name);
  vol = store->rc.lookup_volume(vol_name);
  if (!vol) {
    lderr(store->ctx()) << "ERROR: could not open rados vol" << dendl;
    return -ENOENT;
  }
  return 0;
}

int RGWStateLog::store_entry(const string& client_id, const string& op_id, const string& object,
			     uint32_t state, bufferlist *bl, uint32_t *check_state)
{
  if (client_id.empty() ||
      op_id.empty() ||
      object.empty()) {
    ldout(store->ctx(), 0) << "client_id / op_id / object is empty" << dendl;
  }

  VolumeRef vol;
  int r = open_vol(vol);
  if (r < 0)
    return r;

  string oid = get_oid(object);

  ObjectOperation op(vol->op());
  if (check_state) {
    cls_statelog_check_state(op, client_id, op_id, object, *check_state);
  }
  ceph::real_time ts = ceph::real_clock::now();
  bufferlist nobl;
  cls_statelog_add(op, client_id, op_id, object, ts, state, (bl ? *bl : nobl));
  r = store->rc.objecter->mutate(oid, vol, op);
  if (r < 0) {
    return r;
  }

  return 0;
}

int RGWStateLog::remove_entry(const string& client_id, const string& op_id, const string& object)
{
  if (client_id.empty() ||
      op_id.empty() ||
      object.empty()) {
    ldout(store->ctx(), 0) << "client_id / op_id / object is empty" << dendl;
  }

  VolumeRef vol;
  int r = open_vol(vol);
  if (r < 0)
    return r;

  string oid = get_oid(object);

  ObjectOperation op(vol->op());
  cls_statelog_remove_by_object(op, object, op_id);
  r = store->rc.objecter->mutate(oid, vol, op);
  if (r < 0) {
    return r;
  }

  return 0;
}

void RGWStateLog::init_list_entries(const string& client_id, const string& op_id, const string& object,
				    void **handle)
{
  list_state *state = new list_state;
  state->client_id = client_id;
  state->op_id = op_id;
  state->object = object;
  if (object.empty()) {
    state->cur_shard = 0;
    state->max_shard = num_shards - 1;
  } else {
    state->cur_shard = state->max_shard = get_shard_num(object);
  }
  *handle = (void *)state;
}

int RGWStateLog::list_entries(void *handle, int max_entries,
			      list<cls_statelog_entry>& entries,
			      bool *done)
{
  list_state *state = static_cast<list_state *>(handle);

  VolumeRef vol;
  int r = open_vol(vol);
  if (r < 0)
    return r;

  entries.clear();

  for (; state->cur_shard <= state->max_shard && max_entries > 0; ++state->cur_shard) {
    string oid;
    oid_str(state->cur_shard, oid);

    ObjectOperation op(vol->op());
    list<cls_statelog_entry> ents;
    bool truncated;
    cls_statelog_list(op, state->client_id, state->op_id, state->object,
		      state->marker, max_entries, ents, &state->marker,
		      &truncated);
    r = store->rc.objecter->mutate(oid, vol, op);
    if (r == -ENOENT) {
      truncated = false;
      r = 0;
    }
    if (r < 0) {
      ldout(store->ctx(), 0) << "cls_statelog_list returned " << r << dendl;
      return r;
    }

    if (!truncated) {
      state->marker.clear();
    }

    max_entries -= ents.size();

    entries.splice(entries.end(), ents);

    if (truncated)
      break;
  }

  *done = (state->cur_shard > state->max_shard);

  return 0;
}

void RGWStateLog::finish_list_entries(void *handle)
{
  list_state *state = static_cast<list_state *>(handle);
  delete state;
}

void RGWStateLog::dump_entry(const cls_statelog_entry& entry, Formatter *f)
{
  f->open_object_section("statelog_entry");
  f->dump_string("client_id", entry.client_id);
  f->dump_string("op_id", entry.op_id);
  f->dump_string("object", entry.object);
  f->dump_stream("timestamp") << entry.timestamp;
  if (!dump_entry_internal(entry, f)) {
    f->dump_int("state", entry.state);
  }
  f->close_section();
}

RGWOpState::RGWOpState(RGWRados *_store)
  : RGWStateLog(_store, _store->ctx()->_conf->rgw_num_zone_opstate_shards,
		string("obj_opstate"))
{
}

bool RGWOpState::dump_entry_internal(const cls_statelog_entry& entry, Formatter *f)
{
  string s;
  switch ((OpState)entry.state) {
    case OPSTATE_UNKNOWN:
      s = "unknown";
      break;
    case OPSTATE_IN_PROGRESS:
      s = "in-progress";
      break;
    case OPSTATE_COMPLETE:
      s = "complete";
      break;
    case OPSTATE_ERROR:
      s = "error";
      break;
    case OPSTATE_ABORT:
      s = "abort";
      break;
    case OPSTATE_CANCELLED:
      s = "cancelled";
      break;
    default:
      s = "invalid";
  }
  f->dump_string("state", s);
  return true;
}

int RGWOpState::state_from_str(const string& s, OpState *state)
{
  if (s == "unknown") {
    *state = OPSTATE_UNKNOWN;
  } else if (s == "in-progress") {
    *state = OPSTATE_IN_PROGRESS;
  } else if (s == "complete") {
    *state = OPSTATE_COMPLETE;
  } else if (s == "error") {
    *state = OPSTATE_ERROR;
  } else if (s == "abort") {
    *state = OPSTATE_ABORT;
  } else if (s == "cancelled") {
    *state = OPSTATE_CANCELLED;
  } else {
    return -EINVAL;
  }

  return 0;
}

int RGWOpState::set_state(const string& client_id, const string& op_id, const string& object, OpState state)
{
  uint32_t s = (uint32_t)state;
  return store_entry(client_id, op_id, object, s, NULL, NULL);
}

int RGWOpState::renew_state(const string& client_id, const string& op_id, const string& object, OpState state)
{
  uint32_t s = (uint32_t)state;
  return store_entry(client_id, op_id, object, s, NULL, &s);
}

RGWOpStateSingleOp::RGWOpStateSingleOp(RGWRados *store, const string& cid,
				       const string& oid, const string& obj)
  : os(store), client_id(cid), op_id(oid), object(obj)
{
  cct = store->ctx();
  cur_state = RGWOpState::OPSTATE_UNKNOWN;
}

int RGWOpStateSingleOp::set_state(RGWOpState::OpState state) {
  last_update = ceph::real_clock::now();
  cur_state = state;
  return os.set_state(client_id, op_id, object, state);
}

int RGWOpStateSingleOp::renew_state() {
  ceph::real_time now = ceph::real_clock::now();
  auto rate_limit = cct->_conf->rgw_opstate_ratelimit_time;

  if ((rate_limit > 0ms) && now - last_update < rate_limit) {
    return 0;
  }

  last_update = now;
  return os.renew_state(client_id, op_id, object, cur_state);
}


uint64_t RGWRados::instance_id()
{
  return rc.get_instance_id();
}

uint64_t RGWRados::next_bucket_id()
{
  lock_guard l(bucket_id_lock);
  return ++max_bucket_id;
}

RGWRados *RGWStoreManager::init_storage_provider(CephContext *cct, bool use_gc_thread, bool quota_threads)
{
  int use_cache = cct->_conf->rgw_cache_enabled;
  RGWRados *store = NULL;
  if (!use_cache) {
    store = new RGWRados(cct);
  } else {
    store = new RGWCache<RGWRados>(cct);
  }

  if (store->initialize(use_gc_thread, quota_threads) < 0) {
    delete store;
    return NULL;
  }

  return store;
}

RGWRados *RGWStoreManager::init_raw_storage_provider(CephContext *cct)
{
  RGWRados *store = NULL;
  store = new RGWRados(cct);

  if (store->init_rados() < 0) {
    delete store;
    return NULL;
  }

  return store;
}

void RGWStoreManager::close_storage(RGWRados *store)
{
  if (!store)
    return;

  store->finalize();

  delete store;
}
