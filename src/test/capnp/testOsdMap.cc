// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// #include "gtest/gtest.h"
#include "osd/OSDMap.h"
#include "vol/Volume.h"
#include "cohort/CohortVolume.h"
#include "include/buffer_ptr.h"

#include "global/global_context.h"
#include "global/global_init.h"
#include "common/common_init.h"

#include <capnp/message.h>
#include <capnp/serialize-packed.h>

#include "OSDMap.capnp.h"

#include <iostream>
#include <map>
#include <sys/time.h>

using namespace std;
using namespace kj;

void set_up_volumes(OSDMap &osdmap, int num_volumes) {
  boost::uuids::uuid uuid_memo;
  for (int i = 0; i < num_volumes; ++i) {
    // VolumeRef vr(new CohortVolume(CohortVol));
    // osdmap.create_volume(vr, uuid_memo);
  }
}

void set_up_map(OSDMap &osdmap, int num_osds) {
  boost::uuids::uuid fsid;
  osdmap.build_simple(g_ceph_context, 0, fsid, num_osds);
  OSDMap::Incremental pending_inc(osdmap.get_epoch() + 1);
  pending_inc.fsid = osdmap.get_fsid();
  entity_addr_t sample_addr;
  boost::uuids::uuid sample_uuid;
  for (int i = 0; i < num_osds; ++i) {
    sample_uuid.data[i] = i;
    sample_addr.nonce = i;
    pending_inc.new_state[i] = CEPH_OSD_EXISTS | CEPH_OSD_NEW;
    pending_inc.new_up_client[i] = sample_addr;
    pending_inc.new_up_cluster[i] = sample_addr;
    pending_inc.new_hb_back_up[i] = sample_addr;
    pending_inc.new_hb_front_up[i] = sample_addr;
    pending_inc.new_weight[i] = CEPH_OSD_IN;
    pending_inc.new_uuid[i] = sample_uuid;
  }
  osdmap.apply_incremental(pending_inc);
}


void setUuid(Captain::Uuid::Builder to, const boost::uuids::uuid &from) {
  ::capnp::Data::Reader data(reinterpret_cast<const byte*>(&from),
			     sizeof(from));
  to.setUuid(data);
}


void setEntityAddr(Captain::EntityAddr::Builder to,
		   const entity_addr_t &from) {
  to.setType(from.type);
  to.setNonce(from.nonce);

  ::capnp::Data::Reader addr(reinterpret_cast<const byte*>(&from.addr),
			     sizeof(from.addr));
  to.setAddr(addr);
}


void setListEntityAddr(::capnp::List<Captain::EntityAddr>::Builder to,
		       vector<std::shared_ptr<entity_addr_t> > from) {
  vector<std::shared_ptr<entity_addr_t> >::const_iterator c;
  int i;
  for (c = from.begin(), i = 0; c != from.end(); ++i, ++c) {
    Captain::EntityAddr::Builder to2 = to[i];
    setEntityAddr(to2, **c);
  }
}


void setUTime(Captain::UTime::Builder to, const utime_t &from) {
  Captain::UTime::Tv::Builder tv = to.initTv();
  tv.setTvSec(from.tv.tv_sec);
  tv.setTvNsec(from.tv.tv_nsec);
}


void buildOSDMapMessage(const OSDMap &map,
			Captain::OSDMap::Builder &msg) {
  const int volumeVersion = 0;

  setUuid(msg.initFsid(), map.get_fsid());
  msg.initEpoch().setEpoch(map.get_epoch());
  setUTime(msg.initCreated(), map.get_created());
  setUTime(msg.initModified(), map.get_modified());
  msg.setFlags(map.get_flags());
  msg.setMaxOsd(map.get_max_osd());

  // osd states and weights

  ::capnp::List<uint8_t>::Builder osdState =
      msg.initOsdState(map.get_max_osd());
  ::capnp::List<uint32_t>::Builder osdWeight =
      msg.initOsdWeight(map.get_max_osd());
  ::capnp::List<Captain::OsdInfo>::Builder osdInfos =
      msg.initOsdInfo(map.get_max_osd());
  ::capnp::List<Captain::OsdXInfo>::Builder osdXInfos =
      msg.initOsdXInfo(map.get_max_osd());
  for (int i = 0; i < map.get_max_osd(); ++i) {
    osdState.set(i, map.get_state(i));
    osdWeight.set(i, map.get_weight(i));
    
    Captain::OsdInfo::Builder osdInfo = osdInfos[i];
    osdInfo.initUpFrom().setEpoch(map.get_up_from(i));
    osdInfo.initUpThru().setEpoch(map.get_up_thru(i));
    osdInfo.initDownAt().setEpoch(map.get_down_at(i));

    Captain::OsdXInfo::Builder osdXInfo = osdXInfos[i];
    const osd_xinfo_t &xinfo = map.get_xinfo(i);

    Captain::UTime::Tv::Builder downStamp = osdXInfo.initDownStamp().initTv();
    downStamp.setTvSec(xinfo.down_stamp.tv.tv_sec);
    downStamp.setTvNsec(xinfo.down_stamp.tv.tv_nsec);

    osdXInfo.setLaggyProbability(xinfo.laggy_probability * 0xffffffffu);
    osdXInfo.setLaggyInterval(xinfo.laggy_interval);
    osdXInfo.setFeatures(xinfo.features);

  }

  // volumes

  { // scope block so i and c are only valid for loop
    const std::map<boost::uuids::uuid,VolumeRef> &mapVolumes =
      map.get_volumes();
    ::capnp::List<Captain::Volume>::Builder volumes =
      msg.initVolumes(mapVolumes.size());
    int i;
    std::map<boost::uuids::uuid,VolumeRef>::const_iterator c;
    for (i = 0, c = mapVolumes.begin();
	 c != mapVolumes.end();
	 ++i, ++c) {
      const Volume &v = *(c->second);
      Captain::Volume::Builder v2 = volumes[i];

      v2.setVersion(volumeVersion);
      switch(v.type) {
      case CohortVol:
	v2.setType(Captain::Volume::VolType::COHORT_VOL);
	break;
      case NotAVolType:
	v2.setType(Captain::Volume::VolType::NOT_A_VOL_TYPE);
	break;
      }
      setUuid(v2.initId(), v.id);
      v2.setName(v.name);
      v2.initLastUpdate().setEpoch(v.last_update);
    }
  } // scope block
  
  // osd addrs

  {
    const vector<std::shared_ptr<entity_addr_t> > *addrs;

    addrs = & map.get_osd_addrs()->hb_back_addr;
    setListEntityAddr(msg.initHbBackAddr(addrs->size()), *addrs);

    addrs = & map.get_osd_addrs()->cluster_addr;
    setListEntityAddr(msg.initClusterAddr(addrs->size()), *addrs);

    addrs = & map.get_osd_addrs()->hb_front_addr;
    setListEntityAddr(msg.initHbFrontAddr(addrs->size()), *addrs);
  }

  // blacklist

  { // scope block
    const std::unordered_map<entity_addr_t,utime_t> &blacklist =
      map.get_blacklist();
    ::capnp::List<Captain::EntityAddrUTimePair>::Builder pairs =
      msg.initBlacklist(blacklist.size());

    std::unordered_map<entity_addr_t,utime_t>::const_iterator cfrom;
    ::capnp::List<Captain::EntityAddrUTimePair>::Builder::Iterator cto;
    for (cfrom = blacklist.begin(), cto = pairs.begin();
	 cfrom != blacklist.end() && cto != pairs.end();
	 ++cfrom, ++cto) {
      setEntityAddr(cto->initEntityAddr(), cfrom->first);
      setUTime(cto->initTime(), cfrom->second);
    }
  } // scope block
} // buildOSDMapMessage


long time_diff_milliseconds(timeval start, timeval end) {
  timeval result;
  timersub(&end, &start, &result);
  return result.tv_sec * 1000 + result.tv_usec / 1000;
}


void test_present_encode(const OSDMap &osdMap, int iterations = 1000) {
  timeval start, end;
  gettimeofday(&start, NULL);
  for (int i = 0; i < iterations; ++i) {
    bufferlist bl;
    osdMap.encode(bl);
  }
  gettimeofday(&end, NULL);

  std::cout << "Present encode: " << time_diff_milliseconds(start, end) << " milliseconds for " <<
    iterations << " iterations." << std::endl;
}


typedef kj::ArrayPtr<const kj::ArrayPtr<const ::capnp::word>> segments_t;


inline segments_t map_to_segments(const OSDMap &osdMap) {
    ::capnp::MallocMessageBuilder message;
    Captain::OSDMap::Builder osdMapMsg = message.initRoot<Captain::OSDMap>();
    buildOSDMapMessage(osdMap, osdMapMsg);

    segments_t segments = message.getSegmentsForOutput();

    return segments;
}


void test_capnp_encode(const OSDMap &osdMap, int iterations = 1000) {
  timeval start, end;
  gettimeofday(&start, NULL);
  for (int i = 0; i < iterations; ++i) {
    segments_t segments = map_to_segments(osdMap);
  }
  gettimeofday(&end, NULL);

  std::cout << "Capn Protocol encode: " <<
    time_diff_milliseconds(start, end) << " milliseconds for " <<
    iterations << " iterations." << std::endl;
}


void dump_segments(const segments_t &segments) {
  unsigned long total = 0;
  std::cout << "segments: " << segments.size() << " segments" << std::endl;
  for (const kj::ArrayPtr<const ::capnp::word> *c = segments.begin();
       c != segments.end();
       ++c) {
    total += c->size();
    std::cout << "    size: " << c->size() << " words" << std::endl;
  }

  std::cout << "  segments total: " << total << " words (" <<
    (total * 8) << " bytes)" << std::endl;
}


void dump_bufferlist(const bufferlist &bl) {
  const std::list<bufferptr>& buffers = bl.buffers();
  std::cout << "bufferlist: " << buffers.size() << " buffers" << std::endl;
  for (std::list<bufferptr>::const_iterator it = buffers.begin();
       it != buffers.end();
       ++it) {
    std::cout << "    size: " << it->length() << " bytes" << std::endl;
  }

  std::cout << "  bufferlist total: " << bl.length() <<
    " bytes" << std::endl;
}


int main(int argc, char* argv[]) {
  const int num_osds = 100;
  const int num_volumes = 20;
  const int iterations = 10000;
  OSDMap osdmap;
  std::vector<const char *> preargs;
  std::vector<const char*> args(argv, argv+argc);
  global_init(&preargs, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY,
	      CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  common_init_finish(g_ceph_context);
  // make sure we have 3 copies, or some tests won't work
  g_ceph_context->_conf->set_val("osd_pool_default_size", "3", false);
  // our map is flat, so just try and split across OSDs, not hosts or whatever
  g_ceph_context->_conf->set_val("osd_crush_chooseleaf_type", "0", false);
    
  set_up_map(osdmap, num_osds);
  set_up_volumes(osdmap, num_volumes);

  test_present_encode(osdmap, iterations);
  test_capnp_encode(osdmap, iterations);

  bufferlist bl;
  osdmap.encode(bl);
  dump_bufferlist(bl);
  dump_segments(map_to_segments(osdmap));
}
