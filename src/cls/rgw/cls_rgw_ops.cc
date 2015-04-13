// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "cls/rgw/cls_rgw_ops.h"

#include "common/Formatter.h"
#include "common/ceph_json.h"

void rgw_cls_tag_timeout_op::dump(Formatter *f) const
{
  f->dump_int("tag_timeout", tag_timeout);
}

void rgw_cls_tag_timeout_op::generate_test_instances(list<rgw_cls_tag_timeout_op*>& ls)
{
  ls.push_back(new rgw_cls_tag_timeout_op);
  ls.push_back(new rgw_cls_tag_timeout_op);
  ls.back()->tag_timeout = 23323;
}

void cls_rgw_gc_set_entry_op::dump(Formatter *f) const
{
  f->dump_stream("expiration") << expiration;
  f->open_object_section("obj_info");
  info.dump(f);
  f->close_section();
}

void cls_rgw_gc_set_entry_op::generate_test_instances(list<cls_rgw_gc_set_entry_op*>& ls)
{
  ls.push_back(new cls_rgw_gc_set_entry_op);
  ls.push_back(new cls_rgw_gc_set_entry_op);
  ls.back()->expiration = 123s;
}

void cls_rgw_gc_defer_entry_op::dump(Formatter *f) const
{
  f->dump_stream("expiration_secs") << expiration;
  f->dump_string("tag", tag);
}

void cls_rgw_gc_defer_entry_op::generate_test_instances(
  list<cls_rgw_gc_defer_entry_op*>& ls)
{
  ls.push_back(new cls_rgw_gc_defer_entry_op);
  ls.push_back(new cls_rgw_gc_defer_entry_op);
  ls.back()->expiration = 123s;
  ls.back()->tag = "footag";
}

void cls_rgw_gc_list_op::dump(Formatter *f) const
{
  f->dump_string("marker", marker);
  f->dump_unsigned("max", max);
  f->dump_bool("expired_only", expired_only);
}

void cls_rgw_gc_list_op::generate_test_instances(list<cls_rgw_gc_list_op*>& ls)
{
  ls.push_back(new cls_rgw_gc_list_op);
  ls.push_back(new cls_rgw_gc_list_op);
  ls.back()->marker = "mymarker";
  ls.back()->max = 2312;
}

void cls_rgw_gc_list_ret::dump(Formatter *f) const
{
  encode_json("entries", entries, f);
  f->dump_int("truncated", (int)truncated);
}

void cls_rgw_gc_list_ret::generate_test_instances(list<cls_rgw_gc_list_ret*>& ls)
{
  ls.push_back(new cls_rgw_gc_list_ret);
  ls.push_back(new cls_rgw_gc_list_ret);
  ls.back()->entries.push_back(cls_rgw_gc_obj_info());
  ls.back()->truncated = true;
}


void cls_rgw_gc_remove_op::dump(Formatter *f) const
{
  encode_json("tags", tags, f);
}

void cls_rgw_gc_remove_op::generate_test_instances(list<cls_rgw_gc_remove_op*>& ls)
{
  ls.push_back(new cls_rgw_gc_remove_op);
  ls.push_back(new cls_rgw_gc_remove_op);
  ls.back()->tags.push_back("tag1");
  ls.back()->tags.push_back("tag2");
}

void rgw_cls_obj_prepare_op::generate_test_instances(list<rgw_cls_obj_prepare_op*>& o)
{
  rgw_cls_obj_prepare_op *op = new rgw_cls_obj_prepare_op;
  op->op = CLS_RGW_OP_ADD;
  op->name = "name";
  op->tag = "tag";
  op->locator = "locator";
  o.push_back(op);
  o.push_back(new rgw_cls_obj_prepare_op);
}

void rgw_cls_obj_prepare_op::dump(Formatter *f) const
{
  f->dump_int("op", op);
  f->dump_string("name", name);
  f->dump_string("tag", tag);
  f->dump_string("locator", locator);
}

void rgw_cls_obj_complete_op::generate_test_instances(list<rgw_cls_obj_complete_op*>& o)
{
  rgw_cls_obj_complete_op *op = new rgw_cls_obj_complete_op;
  op->op = CLS_RGW_OP_DEL;
  op->name = "name";
  op->ver.vol = boost::uuids::nil_uuid();
  op->ver.epoch = 100;
  op->tag = "tag";

  list<rgw_bucket_dir_entry_meta *> l;
  rgw_bucket_dir_entry_meta::generate_test_instances(l);
  list<rgw_bucket_dir_entry_meta *>::iterator iter = l.begin();
  op->meta = *(*iter);

  o.push_back(op);

  o.push_back(new rgw_cls_obj_complete_op);
}

void rgw_cls_obj_complete_op::dump(Formatter *f) const
{
  f->dump_int("op", (int)op);
  f->dump_string("name", name);
  f->dump_string("locator", locator);
  f->open_object_section("ver");
  ver.dump(f);
  f->close_section();
  f->open_object_section("meta");
  meta.dump(f);
  f->close_section();
  f->dump_string("tag", tag);
}

void rgw_cls_list_op::generate_test_instances(list<rgw_cls_list_op*>& o)
{
  rgw_cls_list_op *op = new rgw_cls_list_op;
  op->start_obj = "start_obj";
  op->num_entries = 100;
  op->filter_prefix = "filter_prefix";
  o.push_back(op);
  o.push_back(new rgw_cls_list_op);
}

void rgw_cls_list_op::dump(Formatter *f) const
{
  f->dump_string("start_obj", start_obj);
  f->dump_unsigned("num_entries", num_entries);
}

void rgw_cls_list_ret::generate_test_instances(list<rgw_cls_list_ret*>& o)
{
 list<rgw_bucket_dir *> l;
  rgw_bucket_dir::generate_test_instances(l);
  list<rgw_bucket_dir *>::iterator iter;
  for (iter = l.begin(); iter != l.end(); ++iter) {
    rgw_bucket_dir *d = *iter;

    rgw_cls_list_ret *ret = new rgw_cls_list_ret;
    ret->dir = *d;
    ret->is_truncated = true;

    o.push_back(ret);

    delete d;
  }

  o.push_back(new rgw_cls_list_ret);
}

void rgw_cls_list_ret::dump(Formatter *f) const
{
  f->open_object_section("dir");
  dir.dump(f);
  f->close_section();
  f->dump_int("is_truncated", (int)is_truncated);
}

void cls_rgw_bi_log_list_op::dump(Formatter *f) const
{
  f->dump_string("marker", marker);
  f->dump_unsigned("max", max);
}

void cls_rgw_bi_log_list_op::generate_test_instances(list<cls_rgw_bi_log_list_op*>& ls)
{
  ls.push_back(new cls_rgw_bi_log_list_op);
  ls.push_back(new cls_rgw_bi_log_list_op);
  ls.back()->marker = "mark";
  ls.back()->max = 123;
}

void cls_rgw_bi_log_trim_op::dump(Formatter *f) const
{
  f->dump_string("start_marker", start_marker);
  f->dump_string("end_marker", end_marker);
}

void cls_rgw_bi_log_trim_op::generate_test_instances(list<cls_rgw_bi_log_trim_op*>& ls)
{
  ls.push_back(new cls_rgw_bi_log_trim_op);
  ls.push_back(new cls_rgw_bi_log_trim_op);
  ls.back()->start_marker = "foo";
  ls.back()->end_marker = "bar";
}

void cls_rgw_bi_log_list_ret::dump(Formatter *f) const
{
  encode_json("entries", entries, f);
  f->dump_unsigned("truncated", (int)truncated);
}

void cls_rgw_bi_log_list_ret::generate_test_instances(list<cls_rgw_bi_log_list_ret*>& ls)
{
  ls.push_back(new cls_rgw_bi_log_list_ret);
  ls.push_back(new cls_rgw_bi_log_list_ret);
  ls.back()->entries.push_back(rgw_bi_log_entry());
  ls.back()->truncated = true;
}
