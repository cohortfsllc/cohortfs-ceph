// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/types.h"
#include "cls/refcount/cls_refcount_client.h"

#include "gtest/gtest.h"
#include "test/librados/test.h"

#include <errno.h>
#include <string>
#include <vector>

static librados::ObjectWriteOperation *new_op(librados::IoCtx& ioctx) {
  return new librados::ObjectWriteOperation(ioctx);
}

TEST(cls_rgw, test_implicit) /* test refcount using implicit referencing of newly created objects */
{
  librados::Rados rados;
  librados::IoCtx ioctx;
  string volume_name = get_temp_volume_name();

  /* create volume */
  ASSERT_EQ("", create_one_volume_pp(volume_name, rados));
  ASSERT_EQ(0, rados.ioctx_create(volume_name.c_str(), ioctx));

  /* add chains */
  string oid = "obj";


  /* create object */

  ASSERT_EQ(0, ioctx.create(oid, true));

  /* read reference, should return a single wildcard entry */

  list<string> refs;

  ASSERT_EQ(0, cls_refcount_read(ioctx, oid, &refs, true));
  ASSERT_EQ(1, (int)refs.size());

  string wildcard_tag;
  string tag = refs.front();

  ASSERT_EQ(wildcard_tag, tag);

  /* take another reference, verify */

  string oldtag = "oldtag";
  string newtag = "newtag";

  librados::ObjectWriteOperation *op = new_op(ioctx);
  cls_refcount_get(*op, newtag, true);
  ASSERT_EQ(0, ioctx.operate(oid, op));

  ASSERT_EQ(0, cls_refcount_read(ioctx, oid, &refs, true));
  ASSERT_EQ(2, (int)refs.size());

  map<string, bool> refs_map;
  for (list<string>::iterator iter = refs.begin(); iter != refs.end(); ++iter) {
    refs_map[*iter] = true;
  }

  ASSERT_EQ(1, (int)refs_map.count(wildcard_tag));
  ASSERT_EQ(1, (int)refs_map.count(newtag));

  delete op;

  /* drop reference to oldtag */

  op = new_op(ioctx);
  cls_refcount_put(*op, oldtag, true);
  ASSERT_EQ(0, ioctx.operate(oid, op));

  ASSERT_EQ(0, cls_refcount_read(ioctx, oid, &refs, true));
  ASSERT_EQ(1, (int)refs.size());

  tag = refs.front();
  ASSERT_EQ(newtag, tag);

  delete op;

  /* drop oldtag reference again, op should return success, wouldn't do anything */

  op = new_op(ioctx);
  cls_refcount_put(*op, oldtag, true);
  ASSERT_EQ(0, ioctx.operate(oid, op));

  ASSERT_EQ(0, cls_refcount_read(ioctx, oid, &refs, true));
  ASSERT_EQ(1, (int)refs.size());

  tag = refs.front();
  ASSERT_EQ(newtag, tag);

  delete op;

  /* drop newtag reference, make sure object removed */
  op = new_op(ioctx);
  cls_refcount_put(*op, newtag, true);
  ASSERT_EQ(0, ioctx.operate(oid, op));

  ASSERT_EQ(-ENOENT, ioctx.stat(oid, NULL, NULL));

  delete op;

  /* remove volume */
  ioctx.close();
  ASSERT_EQ(0, destroy_one_volume_pp(volume_name, rados));
}

TEST(cls_rgw, test_explicit) /* test refcount using implicit referencing of newly created objects */
{
  librados::Rados rados;
  librados::IoCtx ioctx;
  string volume_name = get_temp_volume_name();

  /* create volume */
  ASSERT_EQ("", create_one_volume_pp(volume_name, rados));
  ASSERT_EQ(0, rados.ioctx_create(volume_name.c_str(), ioctx));

  /* add chains */
  string oid = "obj";


  /* create object */

  ASSERT_EQ(0, ioctx.create(oid, true));

  /* read reference, should return a single wildcard entry */

  list<string> refs;

  ASSERT_EQ(0, cls_refcount_read(ioctx, oid, &refs));
  ASSERT_EQ(0, (int)refs.size());


  /* take first reference, verify */

  string newtag = "newtag";

  librados::ObjectWriteOperation *op = new_op(ioctx);
  cls_refcount_get(*op, newtag);
  ASSERT_EQ(0, ioctx.operate(oid, op));

  ASSERT_EQ(0, cls_refcount_read(ioctx, oid, &refs));
  ASSERT_EQ(1, (int)refs.size());

  map<string, bool> refs_map;
  for (list<string>::iterator iter = refs.begin(); iter != refs.end(); ++iter) {
    refs_map[*iter] = true;
  }

  ASSERT_EQ(1, (int)refs_map.count(newtag));

  delete op;

  /* try to drop reference to unexisting tag */

  string nosuchtag = "nosuchtag";

  op = new_op(ioctx);
  cls_refcount_put(*op, nosuchtag);
  ASSERT_EQ(0, ioctx.operate(oid, op));

  ASSERT_EQ(0, cls_refcount_read(ioctx, oid, &refs));
  ASSERT_EQ(1, (int)refs.size());

  string tag = refs.front();
  ASSERT_EQ(newtag, tag);

  delete op;

  /* drop newtag reference, make sure object removed */
  op = new_op(ioctx);
  cls_refcount_put(*op, newtag);
  ASSERT_EQ(0, ioctx.operate(oid, op));

  ASSERT_EQ(-ENOENT, ioctx.stat(oid, NULL, NULL));

  delete op;

  /* remove volume */
  ioctx.close();
  ASSERT_EQ(0, destroy_one_volume_pp(volume_name, rados));
}

TEST(cls_rgw, set) /* test refcount using implicit referencing of newly created objects */
{
  librados::Rados rados;
  librados::IoCtx ioctx;
  string volume_name = get_temp_volume_name();

  /* create volume */
  ASSERT_EQ("", create_one_volume_pp(volume_name, rados));
  ASSERT_EQ(0, rados.ioctx_create(volume_name.c_str(), ioctx));

  /* add chains */
  string oid = "obj";


  /* create object */

  ASSERT_EQ(0, ioctx.create(oid, true));

  /* read reference, should return a single wildcard entry */

  list<string> tag_refs, refs;

#define TAGS_NUM 5
  string tags[TAGS_NUM];

  char buf[16];
  for (int i = 0; i < TAGS_NUM; i++) {
    snprintf(buf, sizeof(buf), "tag%d", i);
    tags[i] = buf;
    tag_refs.push_back(tags[i]);
  }

  ASSERT_EQ(0, cls_refcount_read(ioctx, oid, &refs));
  ASSERT_EQ(0, (int)refs.size());

  /* set reference list, verify */

  librados::ObjectWriteOperation *op = new_op(ioctx);
  cls_refcount_set(*op, tag_refs);
  ASSERT_EQ(0, ioctx.operate(oid, op));

  refs.clear();
  ASSERT_EQ(0, cls_refcount_read(ioctx, oid, &refs));
  ASSERT_EQ(TAGS_NUM, (int)refs.size());

  map<string, bool> refs_map;
  for (list<string>::iterator iter = refs.begin(); iter != refs.end(); ++iter) {
    refs_map[*iter] = true;
  }

  for (int i = 0; i < TAGS_NUM; i++) {
    ASSERT_EQ(1, (int)refs_map.count(tags[i]));
  }

  delete op;

  /* remove all refs */

  for (int i = 0; i < TAGS_NUM; i++) {
    op = new_op(ioctx);
    cls_refcount_put(*op, tags[i]);
    ASSERT_EQ(0, ioctx.operate(oid, op));
    delete op;
  }

  ASSERT_EQ(-ENOENT, ioctx.stat(oid, NULL, NULL));

  /* remove volume */
  ioctx.close();
  ASSERT_EQ(0, destroy_one_volume_pp(volume_name, rados));
}
