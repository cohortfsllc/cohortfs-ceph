// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/types.h"
#include "cls/log/cls_log_types.h"
#include "cls/log/cls_log_client.h"

#include "gtest/gtest.h"
#include "test/librados/test.h"

#include <errno.h>
#include <string>
#include <vector>

static librados::ObjectWriteOperation *new_op(const librados::IoCtx& ioctx) {
  return new librados::ObjectWriteOperation(ioctx);
}

static librados::ObjectReadOperation *new_rop(const librados::IoCtx& ioctx) {
  return new librados::ObjectReadOperation(ioctx);
}

static void reset_rop(librados::ObjectReadOperation **pop,
		      const librados::IoCtx& ioctx) {
  delete *pop;
  *pop = new_rop(ioctx);
}

static int read_bl(bufferlist& bl, int *i)
{
  bufferlist::iterator iter = bl.begin();

  try {
    ::decode(*i, iter);
  } catch (buffer::error& err) {
    std::cout << "failed to decode buffer" << std::endl;
    return -EIO;
  }

  return 0;
}

void add_log(librados::ObjectWriteOperation *op, ceph::real_time& timestamp,
	     string& section, string& name, int i)
{
  bufferlist bl;
  ::encode(i, bl);

  cls_log_add(*op, timestamp, section, name, bl);
}


string get_name(int i)
{
  string name_prefix = "data-source";

  char buf[16];
  snprintf(buf, sizeof(buf), "%d", i);
  return name_prefix + buf;
}

void generate_log(librados::IoCtx& ioctx, string& oid_t, int max,
		  ceph::real_time& start_time, bool modify_time)
{
  string section = "global";

  librados::ObjectWriteOperation *op = new_op(ioctx);

  int i;

  for (i = 0; i < max; i++) {
    ceph::real_time ts(start_time);
    if (modify_time)
      ts += 1s;

    string name = get_name(i);

    add_log(op, ts, section, name, i);
  }

  ASSERT_EQ(0, ioctx.operate(oid_t, op));

  delete op;
}

ceph::real_time get_time(ceph::real_time& start_time, int i, bool modify_time)
{
  if (modify_time)
    return start_time + 1s;
  else
    return start_time;
}

void check_entry(cls_log_entry& entry, ceph::real_time& start_time, int i, bool modified_time)
{
  string section = "global";
  string name = get_name(i);
  ceph::real_time ts = get_time(start_time, i, modified_time);

  ASSERT_EQ(section, entry.section);
  ASSERT_EQ(name, entry.name);
  ASSERT_EQ(ts, entry.timestamp);
}


TEST(cls_rgw, test_log_add_same_time)
{
  librados::Rados rados;
  librados::IoCtx ioctx;
  string volume_name = get_temp_volume_name();

  /* create volume */
  ASSERT_EQ("", create_one_volume_pp(volume_name, rados));
  ASSERT_EQ(0, rados.ioctx_create(volume_name.c_str(), ioctx));

  /* add chains */
  string oid_t = "oid";

  /* create object */
  ASSERT_EQ(0, ioctx.create(oid_t, true));

  /* generate log */
  ceph::real_time start_time = ceph::real_clock::now();
  generate_log(ioctx, oid_t, 10, start_time, false);

  librados::ObjectReadOperation *rop = new_rop(ioctx);

  list<cls_log_entry> entries;
  bool truncated;

  /* check list */

  ceph::real_time to_time = get_time(start_time, 1, true);

  string marker;

  cls_log_list(*rop, start_time, to_time, marker, 0, entries, &marker, &truncated);

  bufferlist obl;
  ASSERT_EQ(0, ioctx.operate(oid_t, rop, &obl));

  ASSERT_EQ(10, (int)entries.size());
  ASSERT_EQ(0, (int)truncated);

  list<cls_log_entry>::iterator iter;

  /* need to sort returned entries, all were using the same time as key */
  map<int, cls_log_entry> check_ents;

  for (iter = entries.begin(); iter != entries.end(); ++iter) {
    cls_log_entry& entry = *iter;

    int num;
    ASSERT_EQ(0, read_bl(entry.data, &num));

    check_ents[num] = entry;
  }

  ASSERT_EQ(10, (int)check_ents.size());

  map<int, cls_log_entry>::iterator ei;

  /* verify entries are as expected */

  int i;

  for (i = 0, ei = check_ents.begin(); i < 10; i++, ++ei) {
    cls_log_entry& entry = ei->second;

    ASSERT_EQ(i, ei->first);
    check_entry(entry, start_time, i, false);
  }

  reset_rop(&rop, ioctx);

  /* check list again, now want to be truncated*/

  marker.clear();

  cls_log_list(*rop, start_time, to_time, marker, 1, entries, &marker, &truncated);

  ASSERT_EQ(0, ioctx.operate(oid_t, rop, &obl));

  ASSERT_EQ(1, (int)entries.size());
  ASSERT_EQ(1, (int)truncated);

  delete rop;
}

TEST(cls_rgw, test_log_add_different_time)
{
  librados::Rados rados;
  librados::IoCtx ioctx;
  string volume_name = get_temp_volume_name();

  /* create volume */
  ASSERT_EQ("", create_one_volume_pp(volume_name, rados));
  ASSERT_EQ(0, rados.ioctx_create(volume_name.c_str(), ioctx));

  /* add chains */
  string oid_t = "oid";

  /* create object */
  ASSERT_EQ(0, ioctx.create(oid_t, true));

  /* generate log */
  ceph::real_time start_time = ceph::real_clock::now();
  generate_log(ioctx, oid_t, 10, start_time, true);

  librados::ObjectReadOperation *rop = new_rop(ioctx);

  list<cls_log_entry> entries;
  bool truncated;

  ceph::real_time to_time = start_time + 10s;

  string marker;

  /* check list */
  cls_log_list(*rop, start_time, to_time, marker, 0, entries, &marker,
	       &truncated);

  bufferlist obl;
  ASSERT_EQ(0, ioctx.operate(oid_t, rop, &obl));

  ASSERT_EQ(10, (int)entries.size());
  ASSERT_EQ(0, (int)truncated);

  list<cls_log_entry>::iterator iter;

  /* returned entries should be sorted by time */
  map<int, cls_log_entry> check_ents;

  int i;

  for (i = 0, iter = entries.begin(); iter != entries.end(); ++iter, ++i) {
    cls_log_entry& entry = *iter;

    int num;

    ASSERT_EQ(0, read_bl(entry.data, &num));

    ASSERT_EQ(i, num);

    check_entry(entry, start_time, i, true);
  }

  reset_rop(&rop, ioctx);

  /* check list again with shifted time */
  ceph::real_time next_time = get_time(start_time, 1, true);

  marker.clear();

  cls_log_list(*rop, next_time, to_time, marker, 0, entries, &marker, &truncated);

  ASSERT_EQ(0, ioctx.operate(oid_t, rop, &obl));

  ASSERT_EQ(9, (int)entries.size());
  ASSERT_EQ(0, (int)truncated);

  reset_rop(&rop, ioctx);

  marker.clear();

  i = 0;
  do {
    bufferlist obl;
    string old_marker = marker;
    cls_log_list(*rop, start_time, to_time, old_marker, 1, entries, &marker, &truncated);

    ASSERT_EQ(0, ioctx.operate(oid_t, rop, &obl));
    ASSERT_NE(old_marker, marker);
    ASSERT_EQ(1, (int)entries.size());

    ++i;
    ASSERT_GE(10, i);
  } while (truncated);

  ASSERT_EQ(10, i);
  delete rop;
}

TEST(cls_rgw, test_log_trim)
{
  librados::Rados rados;
  librados::IoCtx ioctx;
  string volume_name = get_temp_volume_name();

  /* create volume */
  ASSERT_EQ("", create_one_volume_pp(volume_name, rados));
  ASSERT_EQ(0, rados.ioctx_create(volume_name.c_str(), ioctx));

  /* add chains */
  string oid_t = "oid";

  /* create object */
  ASSERT_EQ(0, ioctx.create(oid_t, true));

  /* generate log */
  ceph::real_time start_time = ceph::real_clock::now();
  generate_log(ioctx, oid_t, 10, start_time, true);

  librados::ObjectReadOperation *rop = new_rop(ioctx);

  list<cls_log_entry> entries;
  bool truncated;

  /* check list */

  /* trim */
  ceph::real_time to_time = get_time(start_time, 10, true);

  for (int i = 0; i < 10; i++) {
    ceph::real_time trim_time = get_time(start_time, i, true);

    ceph::real_time zero_time;
    string start_marker, end_marker;

    ASSERT_EQ(0, cls_log_trim(ioctx, oid_t, zero_time, trim_time, start_marker, end_marker));

    string marker;

    cls_log_list(*rop, start_time, to_time, marker, 0, entries, &marker, &truncated);

    bufferlist obl;
    ASSERT_EQ(0, ioctx.operate(oid_t, rop, &obl));

    ASSERT_EQ(9 - i, (int)entries.size());
    ASSERT_EQ(0, (int)truncated);
  }
  delete rop;
}
