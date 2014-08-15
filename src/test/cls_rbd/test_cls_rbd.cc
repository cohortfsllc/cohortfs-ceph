// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/encoding.h"
#include "include/types.h"
#include "include/rados/librados.h"
#include "cls/rbd/cls_rbd_client.h"

#include "gtest/gtest.h"
#include "test/librados/test.h"

#include <errno.h>
#include <string>
#include <vector>

using namespace std;
using ::librbd::cls_client::create_image;
using ::librbd::cls_client::get_features;
using ::librbd::cls_client::get_size;
using ::librbd::cls_client::get_object_prefix;
using ::librbd::cls_client::set_size;
using ::librbd::cls_client::get_id;
using ::librbd::cls_client::set_id;
using ::librbd::cls_client::dir_get_id;
using ::librbd::cls_client::dir_get_name;
using ::librbd::cls_client::dir_list;
using ::librbd::cls_client::dir_add_image;
using ::librbd::cls_client::dir_remove_image;
using ::librbd::cls_client::dir_rename_image;
using ::librbd::cls_client::get_stripe_unit_count;
using ::librbd::cls_client::set_stripe_unit_count;

TEST(cls_rbd, get_and_set_id)
{
  librados::Rados rados;
  librados::IoCtx ioctx;
  string pool_name = get_temp_pool_name();

  ASSERT_EQ("", create_one_pool_pp(pool_name, rados));
  ASSERT_EQ(0, rados.ioctx_create(pool_name.c_str(), ioctx));

  string oid = "rbd_id_test";
  string id;
  string valid_id = "0123abcxyzZYXCBA";
  string invalid_id = ".abc";
  string empty_id;

  ASSERT_EQ(-ENOENT, get_id(&ioctx, oid, &id));
  ASSERT_EQ(-ENOENT, set_id(&ioctx, oid, valid_id));

  ASSERT_EQ(0, ioctx.create(oid, true));
  ASSERT_EQ(-EINVAL, set_id(&ioctx, oid, invalid_id));
  ASSERT_EQ(-EINVAL, set_id(&ioctx, oid, empty_id));
  ASSERT_EQ(-ENOENT, get_id(&ioctx, oid, &id));

  ASSERT_EQ(0, set_id(&ioctx, oid, valid_id));
  ASSERT_EQ(-EEXIST, set_id(&ioctx, oid, valid_id));
  ASSERT_EQ(-EEXIST, set_id(&ioctx, oid, valid_id + valid_id));
  ASSERT_EQ(0, get_id(&ioctx, oid, &id));
  ASSERT_EQ(id, valid_id);

  ioctx.close();
  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, rados));
}

TEST(cls_rbd, directory_methods)
{
  librados::Rados rados;
  librados::IoCtx ioctx;
  string pool_name = get_temp_pool_name();

  ASSERT_EQ("", create_one_pool_pp(pool_name, rados));
  ASSERT_EQ(0, rados.ioctx_create(pool_name.c_str(), ioctx));

  string oid = "rbd_id_test";
  string id, name;
  string imgname = "bar";
  string imgname2 = "foo";
  string imgname3 = "baz";
  string valid_id = "0123abcxyzZYXCBA";
  string valid_id2 = "5";
  string invalid_id = ".abc";
  string empty;

  ASSERT_EQ(-ENOENT, dir_get_id(&ioctx, oid, imgname, &id));
  ASSERT_EQ(-ENOENT, dir_get_name(&ioctx, oid, valid_id, &name));
  ASSERT_EQ(-ENOENT, dir_remove_image(&ioctx, oid, imgname, valid_id));

  ASSERT_EQ(-EINVAL, dir_add_image(&ioctx, oid, imgname, invalid_id));
  ASSERT_EQ(-EINVAL, dir_add_image(&ioctx, oid, imgname, empty));
  ASSERT_EQ(-EINVAL, dir_add_image(&ioctx, oid, empty, valid_id));

  map<string, string> images;
  ASSERT_EQ(-ENOENT, dir_list(&ioctx, oid, "", 30, &images));

  ASSERT_EQ(0, ioctx.create(oid, true));
  ASSERT_EQ(0, dir_list(&ioctx, oid, "", 30, &images));
  ASSERT_EQ(0u, images.size());
  ASSERT_EQ(0, ioctx.remove(oid));

  ASSERT_EQ(0, dir_add_image(&ioctx, oid, imgname, valid_id));
  ASSERT_EQ(-EEXIST, dir_add_image(&ioctx, oid, imgname, valid_id2));
  ASSERT_EQ(-EBADF, dir_add_image(&ioctx, oid, imgname2, valid_id));
  ASSERT_EQ(0, dir_list(&ioctx, oid, "", 30, &images));
  ASSERT_EQ(1u, images.size());
  ASSERT_EQ(valid_id, images[imgname]);
  ASSERT_EQ(0, dir_list(&ioctx, oid, "", 0, &images));
  ASSERT_EQ(0u, images.size());
  ASSERT_EQ(0, dir_get_name(&ioctx, oid, valid_id, &name));
  ASSERT_EQ(imgname, name);
  ASSERT_EQ(0, dir_get_id(&ioctx, oid, imgname, &id));
  ASSERT_EQ(valid_id, id);

  ASSERT_EQ(0, dir_add_image(&ioctx, oid, imgname2, valid_id2));
  ASSERT_EQ(0, dir_list(&ioctx, oid, "", 30, &images));
  ASSERT_EQ(2u, images.size());
  ASSERT_EQ(valid_id, images[imgname]);
  ASSERT_EQ(valid_id2, images[imgname2]);
  ASSERT_EQ(0, dir_list(&ioctx, oid, imgname, 0, &images));
  ASSERT_EQ(0u, images.size());
  ASSERT_EQ(0, dir_list(&ioctx, oid, imgname, 2, &images));
  ASSERT_EQ(1u, images.size());
  ASSERT_EQ(valid_id2, images[imgname2]);
  ASSERT_EQ(0, dir_get_name(&ioctx, oid, valid_id2, &name));
  ASSERT_EQ(imgname2, name);
  ASSERT_EQ(0, dir_get_id(&ioctx, oid, imgname2, &id));
  ASSERT_EQ(valid_id2, id);

  ASSERT_EQ(-ESTALE, dir_rename_image(&ioctx, oid, imgname, imgname2, valid_id2));
  ASSERT_EQ(-ESTALE, dir_remove_image(&ioctx, oid, imgname, valid_id2));
  ASSERT_EQ(-EEXIST, dir_rename_image(&ioctx, oid, imgname, imgname2, valid_id));
  ASSERT_EQ(0, dir_get_id(&ioctx, oid, imgname, &id));
  ASSERT_EQ(valid_id, id);
  ASSERT_EQ(0, dir_get_name(&ioctx, oid, valid_id2, &name));
  ASSERT_EQ(imgname2, name);

  ASSERT_EQ(0, dir_rename_image(&ioctx, oid, imgname, imgname3, valid_id));
  ASSERT_EQ(0, dir_get_id(&ioctx, oid, imgname3, &id));
  ASSERT_EQ(valid_id, id);
  ASSERT_EQ(0, dir_get_name(&ioctx, oid, valid_id, &name));
  ASSERT_EQ(imgname3, name);
  ASSERT_EQ(0, dir_rename_image(&ioctx, oid, imgname3, imgname, valid_id));

  ASSERT_EQ(0, dir_remove_image(&ioctx, oid, imgname, valid_id));
  ASSERT_EQ(0, dir_list(&ioctx, oid, "", 30, &images));
  ASSERT_EQ(1u, images.size());
  ASSERT_EQ(valid_id2, images[imgname2]);
  ASSERT_EQ(0, dir_list(&ioctx, oid, imgname2, 30, &images));
  ASSERT_EQ(0u, images.size());
  ASSERT_EQ(0, dir_get_name(&ioctx, oid, valid_id2, &name));
  ASSERT_EQ(imgname2, name);
  ASSERT_EQ(0, dir_get_id(&ioctx, oid, imgname2, &id));
  ASSERT_EQ(valid_id2, id);
  ASSERT_EQ(-ENOENT, dir_get_name(&ioctx, oid, valid_id, &name));
  ASSERT_EQ(-ENOENT, dir_get_id(&ioctx, oid, imgname, &id));

  ASSERT_EQ(0, dir_add_image(&ioctx, oid, imgname, valid_id));
  ASSERT_EQ(0, dir_list(&ioctx, oid, "", 30, &images));
  ASSERT_EQ(2u, images.size());
  ASSERT_EQ(valid_id, images[imgname]);
  ASSERT_EQ(valid_id2, images[imgname2]);
  ASSERT_EQ(0, dir_remove_image(&ioctx, oid, imgname, valid_id));
  ASSERT_EQ(-ENOENT, dir_remove_image(&ioctx, oid, imgname, valid_id));
  ASSERT_EQ(0, dir_remove_image(&ioctx, oid, imgname2, valid_id2));
  ASSERT_EQ(0, dir_list(&ioctx, oid, "", 30, &images));
  ASSERT_EQ(0u, images.size());

  ioctx.close();
  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, rados));
}

TEST(cls_rbd, create)
{
  librados::Rados rados;
  librados::IoCtx ioctx;
  string pool_name = get_temp_pool_name();

  ASSERT_EQ("", create_one_pool_pp(pool_name, rados));
  ASSERT_EQ(0, rados.ioctx_create(pool_name.c_str(), ioctx));

  string oid = "testobj";
  uint64_t size = 20ULL << 30;
  uint64_t features = 0;
  uint8_t order = 22;
  string object_prefix = "foo";

  ASSERT_EQ(0, create_image(&ioctx, oid, size, order,
			    features, object_prefix));
  ASSERT_EQ(-EEXIST, create_image(&ioctx, oid, size, order,
				  features, object_prefix));
  ASSERT_EQ(0, ioctx.remove(oid));

  ASSERT_EQ(-EINVAL, create_image(&ioctx, oid, size, order,
				  features, ""));
  ASSERT_EQ(-ENOENT, ioctx.remove(oid));

  ASSERT_EQ(0, create_image(&ioctx, oid, 0, order,
			    features, object_prefix));
  ASSERT_EQ(0, ioctx.remove(oid));

  ASSERT_EQ(-ENOSYS, create_image(&ioctx, oid, size, order,
				  -1, object_prefix));
  ASSERT_EQ(-ENOENT, ioctx.remove(oid));

  bufferlist inbl, outbl;
  ASSERT_EQ(-EINVAL, ioctx.exec(oid, "rbd", "create", inbl, outbl));

  ioctx.close();
  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, rados));
}

TEST(cls_rbd, get_features)
{
  librados::Rados rados;
  librados::IoCtx ioctx;
  string pool_name = get_temp_pool_name();

  ASSERT_EQ("", create_one_pool_pp(pool_name, rados));
  ASSERT_EQ(0, rados.ioctx_create(pool_name.c_str(), ioctx));

  uint64_t features;
  ASSERT_EQ(-ENOENT, get_features(&ioctx, "foo", &features));

  ASSERT_EQ(0, create_image(&ioctx, "foo", 0, 22, 0, "foo"));
  ASSERT_EQ(0, get_features(&ioctx, "foo", &features));
  ASSERT_EQ(0u, features);

  ioctx.close();
  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, rados));
}

TEST(cls_rbd, get_object_prefix)
{
  librados::Rados rados;
  librados::IoCtx ioctx;
  string pool_name = get_temp_pool_name();

  ASSERT_EQ("", create_one_pool_pp(pool_name, rados));
  ASSERT_EQ(0, rados.ioctx_create(pool_name.c_str(), ioctx));

  string object_prefix;
  ASSERT_EQ(-ENOENT, get_object_prefix(&ioctx, "foo", &object_prefix));

  ASSERT_EQ(0, create_image(&ioctx, "foo", 0, 22, 0, "foo"));
  ASSERT_EQ(0, get_object_prefix(&ioctx, "foo", &object_prefix));
  ASSERT_EQ("foo", object_prefix);

  ioctx.close();
  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, rados));
}

TEST(cls_rbd, get_size)
{
  librados::Rados rados;
  librados::IoCtx ioctx;
  string pool_name = get_temp_pool_name();

  ASSERT_EQ("", create_one_pool_pp(pool_name, rados));
  ASSERT_EQ(0, rados.ioctx_create(pool_name.c_str(), ioctx));

  uint64_t size;
  uint8_t order;
  ASSERT_EQ(-ENOENT, get_size(&ioctx, "foo", &size, &order));

  ASSERT_EQ(0, create_image(&ioctx, "foo", 0, 22, 0, "foo"));
  ASSERT_EQ(0, get_size(&ioctx, "foo", &size, &order));
  ASSERT_EQ(0u, size);
  ASSERT_EQ(22, order);
  ASSERT_EQ(0, ioctx.remove("foo"));

  ASSERT_EQ(0, create_image(&ioctx, "foo", 2 << 22, 0, 0, "foo"));
  ASSERT_EQ(0, get_size(&ioctx, "foo", &size, &order));
  ASSERT_EQ(2u << 22, size);
  ASSERT_EQ(0, order);

  ioctx.close();
  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, rados));
}

TEST(cls_rbd, set_size)
{
  librados::Rados rados;
  librados::IoCtx ioctx;
  string pool_name = get_temp_pool_name();

  ASSERT_EQ("", create_one_pool_pp(pool_name, rados));
  ASSERT_EQ(0, rados.ioctx_create(pool_name.c_str(), ioctx));

  ASSERT_EQ(-ENOENT, set_size(&ioctx, "foo", 5));

  uint64_t size;
  uint8_t order;
  ASSERT_EQ(0, create_image(&ioctx, "foo", 0, 22, 0, "foo"));
  ASSERT_EQ(0, get_size(&ioctx, "foo", &size, &order));
  ASSERT_EQ(0u, size);
  ASSERT_EQ(22, order);

  ASSERT_EQ(0, set_size(&ioctx, "foo", 0));
  ASSERT_EQ(0, get_size(&ioctx, "foo", &size, &order));
  ASSERT_EQ(0u, size);
  ASSERT_EQ(22, order);

  ASSERT_EQ(0, set_size(&ioctx, "foo", 3 << 22));
  ASSERT_EQ(0, get_size(&ioctx, "foo", &size, &order));
  ASSERT_EQ(3u << 22, size);
  ASSERT_EQ(22, order);

  ioctx.close();
  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, rados));
}

TEST(cls_rbd, stripingv2)
{
  librados::Rados rados;
  librados::IoCtx ioctx;
  string pool_name = get_temp_pool_name();

  ASSERT_EQ("", create_one_pool_pp(pool_name, rados));
  ASSERT_EQ(0, rados.ioctx_create(pool_name.c_str(), ioctx));

  ASSERT_EQ(0, create_image(&ioctx, "foo", 10, 22, 0, "foo"));

  uint64_t su = 65536, sc = 12;
  ASSERT_EQ(-ENOEXEC, get_stripe_unit_count(&ioctx, "foo", &su, &sc));
  ASSERT_EQ(-ENOEXEC, set_stripe_unit_count(&ioctx, "foo", su, sc));

  ASSERT_EQ(0, create_image(&ioctx, "bar", 10, 22, RBD_FEATURE_STRIPINGV2, "bar"));
  ASSERT_EQ(0, get_stripe_unit_count(&ioctx, "bar", &su, &sc));
  ASSERT_EQ(1ull << 22, su);
  ASSERT_EQ(1ull, sc);
  su = 8192;
  sc = 456;
  ASSERT_EQ(0, set_stripe_unit_count(&ioctx, "bar", su, sc));
  su = sc = 0;
  ASSERT_EQ(0, get_stripe_unit_count(&ioctx, "bar", &su, &sc));
  ASSERT_EQ(8192ull, su);
  ASSERT_EQ(456ull, sc);

  // su must not be larger than an object
  ASSERT_EQ(-EINVAL, set_stripe_unit_count(&ioctx, "bar", 1 << 23, 1));
  // su must be a factor of object size
  ASSERT_EQ(-EINVAL, set_stripe_unit_count(&ioctx, "bar", 511, 1));
  // su and sc must be non-zero
  ASSERT_EQ(-EINVAL, set_stripe_unit_count(&ioctx, "bar", 0, 1));
  ASSERT_EQ(-EINVAL, set_stripe_unit_count(&ioctx, "bar", 1, 0));
  ASSERT_EQ(-EINVAL, set_stripe_unit_count(&ioctx, "bar", 0, 0));

  ioctx.close();
  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, rados));
}
