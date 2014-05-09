// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/** \file
 *
 * This is an OSD class that implements methods for
 * use with rbd.
 *
 * Most of these deal with the rbd header object. Methods prefixed
 * with old_ deal with the original rbd design, in which clients read
 * and interpreted the header object directly.
 *
 * The new format is meant to be opaque to clients - all their
 * interactions with non-data objects should go through this
 * class. The OSD class interface leaves the class to implement its
 * own argument and payload serialization/deserialization, so for ease
 * of implementation we use the existing ceph encoding/decoding
 * methods. Something like json might be preferable, but the rbd
 * kernel module has to be able understand format as well. The
 * datatypes exposed to the clients are strings, unsigned integers,
 * and vectors of those types. The on-wire format can be found in
 * src/include/encoding.h.
 *
 * The methods for interacting with the new format document their
 * parameters as the client sees them - it would be silly to mention
 * in each one that they take an input and an output bufferlist.
 */
#include "include/types.h"

#include <algorithm>
#include <cstring>
#include <cstdlib>
#include <errno.h>
#include <iostream>
#include <map>
#include <sstream>
#include <vector>

#include "common/errno.h"
#include "objclass/objclass.h"
#include "include/rbd_types.h"


/*
 * Object keys:
 *
 * <partial list>
 *
 * stripe_unit: size in bytes of the stripe unit.  if not present,
 *   the stripe unit is assumed to match the object size (1 << order).
 *
 * stripe_count: number of objects to stripe over before looping back.
 *   if not present or 1, striping is disabled.  this is the default.
 *
 */

CLS_VER(2,0)
CLS_NAME(rbd)

cls_handle_t h_class;
cls_method_handle_t h_create;
cls_method_handle_t h_get_features;
cls_method_handle_t h_get_size;
cls_method_handle_t h_set_size;
cls_method_handle_t h_get_stripe_unit_count;
cls_method_handle_t h_set_stripe_unit_count;
cls_method_handle_t h_get_object_prefix;
cls_method_handle_t h_get_all_features;
cls_method_handle_t h_get_id;
cls_method_handle_t h_set_id;
cls_method_handle_t h_dir_get_id;
cls_method_handle_t h_dir_get_name;
cls_method_handle_t h_dir_list;
cls_method_handle_t h_dir_add_image;
cls_method_handle_t h_dir_remove_image;
cls_method_handle_t h_dir_rename_image;

#define RBD_MAX_KEYS_READ 64
#define RBD_DIR_ID_KEY_PREFIX "id_"
#define RBD_DIR_NAME_KEY_PREFIX "name_"

template<typename T>
static int read_key(cls_method_context_t hctx, const string &key, T *out)
{
  bufferlist bl;
  int r = cls_cxx_map_get_val(hctx, key, &bl);
  if (r < 0) {
    if (r != -ENOENT) {
      CLS_ERR("error reading omap key %s: %d", key.c_str(), r);
    }
    return r;
  }

  try {
    bufferlist::iterator it = bl.begin();
    ::decode(*out, it);
  } catch (const buffer::error &err) {
    CLS_ERR("error decoding %s", key.c_str());
    return -EIO;
  }

  return 0;
}

static bool is_valid_id(const string &id) {
  if (!id.size())
    return false;
  for (size_t i = 0; i < id.size(); ++i) {
    if (!isalnum(id[i])) {
      return false;
    }
  }
  return true;
}

/**
 * Initialize the header with basic metadata.
 * Extra features may initialize more fields in the future.
 * Everything is stored as key/value pairs as omaps in the header object.
 *
 * If features the OSD does not understand are requested, -ENOSYS is
 * returned.
 *
 * Input:
 * @param size number of bytes in the image (uint64_t)
 * @param order bits to shift to determine the size of data objects (uint8_t)
 * @param features what optional things this image will use (uint64_t)
 * @param object_prefix a prefix for all the data objects
 *
 * Output:
 * @return 0 on success, negative error code on failure
 */
int create(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  string object_prefix;
  uint64_t features, size;
  uint8_t order;

  try {
    bufferlist::iterator iter = in->begin();
    ::decode(size, iter);
    ::decode(order, iter);
    ::decode(features, iter);
    ::decode(object_prefix, iter);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  CLS_LOG(20,
	  "create object_prefix=%s size=%"PRIu64" order=%u features=%"PRIu64,
	  object_prefix.c_str(), size, order, features);

  if (features & ~RBD_FEATURES_ALL) {
    return -ENOSYS;
  }

  if (!object_prefix.size()) {
    return -EINVAL;
  }

  bufferlist stored_prefixbl;
  int r = cls_cxx_map_get_val(hctx, "object_prefix", &stored_prefixbl);
  if (r != -ENOENT) {
    CLS_ERR("reading object_prefix returned %d", r);
    return -EEXIST;
  }

  bufferlist sizebl;
  ::encode(size, sizebl);
  r = cls_cxx_map_set_val(hctx, "size", &sizebl);
  if (r < 0)
    return r;

  bufferlist orderbl;
  ::encode(order, orderbl);
  r = cls_cxx_map_set_val(hctx, "order", &orderbl);
  if (r < 0)
    return r;

  bufferlist featuresbl;
  ::encode(features, featuresbl);
  r = cls_cxx_map_set_val(hctx, "features", &featuresbl);
  if (r < 0)
    return r;

  bufferlist object_prefixbl;
  ::encode(object_prefix, object_prefixbl);
  r = cls_cxx_map_set_val(hctx, "object_prefix", &object_prefixbl);
  if (r < 0)
    return r;

  return 0;
}

/**
 * Input:
 *
 * Output:
 * @param features list of enabled features for the given image
 * @returns 0 on success, negative error code on failure
 */
int get_features(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  uint64_t features;

  int r = read_key(hctx, "features", &features);
  if (r < 0) {
    CLS_ERR("failed to read features off disk: %s", cpp_strerror(r).c_str());
    return r;
  }

  uint64_t incompatible = features & RBD_FEATURES_INCOMPATIBLE;
  ::encode(features, *out);
  ::encode(incompatible, *out);

  return 0;
}

/**
 * check that given feature(s) are set
 *
 * @param hctx context
 * @param need features needed
 * @return 0 if features are set, negative error (like ENOEXEC) otherwise
 */
int require_feature(cls_method_context_t hctx, uint64_t need)
{
  uint64_t features;
  int r = read_key(hctx, "features", &features);
  if (r == -ENOENT)   // this implies it's an old-style image with no features
    return -ENOEXEC;
  if (r < 0)
    return r;
  if ((features & need) != need) {
    CLS_LOG(10, "require_feature missing feature %"PRIx64", have %"PRIx64,
	    need, features);
    return -ENOEXEC;
  }
  return 0;
}

/**
 * Input:
 *
 * Output:
 * @param order bits to shift to get the size of data objects (uint8_t)
 * @param size size of the image in bytes
 * @returns 0 on success, negative error code on failure
 */
int get_size(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  uint64_t size;
  uint8_t order;

  int r = read_key(hctx, "order", &order);
  if (r < 0) {
    CLS_ERR("failed to read the order off of disk: %s",
	    cpp_strerror(r).c_str());
    return r;
  }

  r = read_key(hctx, "size", &size);
  if (r < 0) {
    CLS_ERR("failed to read the image's size off of disk: %s",
	    cpp_strerror(r).c_str());
    return r;
  }

  ::encode(order, *out);
  ::encode(size, *out);

  return 0;
}

/**
 * Input:
 * @param size new capacity of the image in bytes (uint64_t)
 *
 * Output:
 * @returns 0 on success, negative error code on failure
 */
int set_size(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  uint64_t size;

  bufferlist::iterator iter = in->begin();
  try {
    ::decode(size, iter);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  // check that size exists to make sure this is a header object
  // that was created correctly
  uint64_t orig_size;
  int r = read_key(hctx, "size", &orig_size);
  if (r < 0) {
    CLS_ERR("Could not read image's size off disk: %s",
	    cpp_strerror(r).c_str());
    return r;
  }

  CLS_LOG(20, "set_size size=%"PRIu64" orig_size=%"PRIu64, size, orig_size);

  bufferlist sizebl;
  ::encode(size, sizebl);
  r = cls_cxx_map_set_val(hctx, "size", &sizebl);
  if (r < 0) {
    CLS_ERR("error writing metadata: %d", r);
    return r;
  }

  return 0;
}

/**
 * verify that the header object exists
 *
 * @return 0 if the object exists, -ENOENT if it does not, or other error
 */
int check_exists(cls_method_context_t hctx)
{
  uint64_t size;
  time_t mtime;
  return cls_cxx_stat(hctx, &size, &mtime);
}

/**
 * get striping parameters
 *
 * Input:
 * none
 *
 * Output:
 * @param stripe unit (bytes)
 * @param stripe count (num objects)
 *
 * @returns 0 on success
 */
int get_stripe_unit_count(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  int r = check_exists(hctx);
  if (r < 0)
    return r;

  CLS_LOG(20, "get_stripe_unit_count");

  r = require_feature(hctx, RBD_FEATURE_STRIPINGV2);
  if (r < 0)
    return r;

  uint64_t stripe_unit = 0, stripe_count = 0;
  r = read_key(hctx, "stripe_unit", &stripe_unit);
  if (r == -ENOENT) {
    // default to object size
    uint8_t order;
    r = read_key(hctx, "order", &order);
    if (r < 0) {
      CLS_ERR("failed to read the order off of disk: %s", cpp_strerror(r).c_str());
      return -EIO;
    }
    stripe_unit = 1ull << order;
  }
  if (r < 0)
    return r;
  r = read_key(hctx, "stripe_count", &stripe_count);
  if (r == -ENOENT) {
    // default to 1
    stripe_count = 1;
    r = 0;
  }
  if (r < 0)
    return r;

  ::encode(stripe_unit, *out);
  ::encode(stripe_count, *out);
  return 0;
}

/**
 * set striping parameters
 *
 * Input:
 * @param stripe unit (bytes)
 * @param stripe count (num objects)
 *
 * @returns 0 on success
 */
int set_stripe_unit_count(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  uint64_t stripe_unit, stripe_count;

  bufferlist::iterator iter = in->begin();
  try {
    ::decode(stripe_unit, iter);
    ::decode(stripe_count, iter);
  } catch (const buffer::error &err) {
    CLS_LOG(20, "set_stripe_unit_count: invalid decode");
    return -EINVAL;
  }

  if (!stripe_count || !stripe_unit)
    return -EINVAL;

  int r = check_exists(hctx);
  if (r < 0)
    return r;

  CLS_LOG(20, "set_stripe_unit_count");

  r = require_feature(hctx, RBD_FEATURE_STRIPINGV2);
  if (r < 0)
    return r;

  uint8_t order;
  r = read_key(hctx, "order", &order);
  if (r < 0) {
    CLS_ERR("failed to read the order off of disk: %s",
	    cpp_strerror(r).c_str());
    return r;
  }
  if ((1ull << order) % stripe_unit || stripe_unit > (1ull << order)) {
    CLS_ERR("stripe unit %"PRIu64" is not a factor of the object size %llu",
	    stripe_unit, 1ull << order);
    return -EINVAL;
  }

  bufferlist bl, bl2;
  ::encode(stripe_unit, bl);
  r = cls_cxx_map_set_val(hctx, "stripe_unit", &bl);
  if (r < 0) {
    CLS_ERR("error writing stripe_unit metadata: %d", r);
    return r;
  }

  ::encode(stripe_count, bl2);
  r = cls_cxx_map_set_val(hctx, "stripe_count", &bl2);
  if (r < 0) {
    CLS_ERR("error writing stripe_count metadata: %d", r);
    return r;
  }

  return 0;
}


/**
 * Output:
 * @param object_prefix prefix for data object names (string)
 * @returns 0 on success, negative error code on failure
 */
int get_object_prefix(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  CLS_LOG(20, "get_object_prefix");

  string object_prefix;
  int r = read_key(hctx, "object_prefix", &object_prefix);
  if (r < 0) {
    CLS_ERR("failed to read the image's object prefix off of disk: %s",
	    cpp_strerror(r).c_str());
    return r;
  }

  ::encode(object_prefix, *out);

  return 0;
}

/**
 * Returns a uint64_t of all the features supported by this class.
 */
int get_all_features(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  uint64_t all_features = RBD_FEATURES_ALL;
  ::encode(all_features, *out);
  return 0;
}

/************************ rbd_id object methods **************************/

/**
 * Input:
 * @param in ignored
 *
 * Output:
 * @param id the id stored in the object
 * @returns 0 on success, negative error code on failure
 */
int get_id(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  uint64_t size;
  int r = cls_cxx_stat(hctx, &size, NULL);
  if (r < 0)
    return r;

  if (size == 0)
    return -ENOENT;

  bufferlist read_bl;
  r = cls_cxx_read(hctx, 0, size, &read_bl);
  if (r < 0) {
    CLS_ERR("get_id: could not read id: %d", r);
    return r;
  }

  string id;
  try {
    bufferlist::iterator iter = read_bl.begin();
    ::decode(id, iter);
  } catch (const buffer::error &err) {
    return -EIO;
  }

  ::encode(id, *out);
  return 0;
};

/**
 * Set the id of an image. The object must already exist.
 *
 * Input:
 * @param id the id of the image, as an alpha-numeric string
 *
 * Output:
 * @returns 0 on success, -EEXIST if the atomic create fails,
 *          negative error code on other error
 */
int set_id(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  int r = check_exists(hctx);
  if (r < 0)
    return r;

  string id;
  try {
    bufferlist::iterator iter = in->begin();
    ::decode(id, iter);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  if (!is_valid_id(id)) {
    CLS_ERR("set_id: invalid id '%s'", id.c_str());
    return -EINVAL;
  }

  uint64_t size;
  r = cls_cxx_stat(hctx, &size, NULL);
  if (r < 0)
    return r;
  if (size != 0)
    return -EEXIST;

  CLS_LOG(20, "set_id: id=%s", id.c_str());

  bufferlist write_bl;
  ::encode(id, write_bl);
  return cls_cxx_write(hctx, 0, write_bl.length(), &write_bl);
}

/*********************** methods for rbd_directory ***********************/

static const string dir_key_for_id(const string &id)
{
  return RBD_DIR_ID_KEY_PREFIX + id;
}

static const string dir_key_for_name(const string &name)
{
  return RBD_DIR_NAME_KEY_PREFIX + name;
}

static const string dir_name_from_key(const string &key)
{
  return key.substr(strlen(RBD_DIR_NAME_KEY_PREFIX));
}

static int dir_add_image_helper(cls_method_context_t hctx,
				const string &name, const string &id,
				bool check_for_unique_id)
{
  if (!name.size() || !is_valid_id(id)) {
    CLS_ERR("dir_add_image_helper: invalid name '%s' or id '%s'",
	    name.c_str(), id.c_str());
    return -EINVAL;
  }

  CLS_LOG(20, "dir_add_image_helper name=%s id=%s", name.c_str(), id.c_str());

  string tmp;
  string name_key = dir_key_for_name(name);
  string id_key = dir_key_for_id(id);
  int r = read_key(hctx, name_key, &tmp);
  if (r != -ENOENT) {
    CLS_LOG(10, "name already exists");
    return -EEXIST;
  }
  r = read_key(hctx, id_key, &tmp);
  if (r != -ENOENT && check_for_unique_id) {
    CLS_LOG(10, "id already exists");
    return -EBADF;
  }
  bufferlist id_bl, name_bl;
  ::encode(id, id_bl);
  ::encode(name, name_bl);
  map<string, bufferlist> omap_vals;
  omap_vals[name_key] = id_bl;
  omap_vals[id_key] = name_bl;
  return cls_cxx_map_set_vals(hctx, &omap_vals);
}

static int dir_remove_image_helper(cls_method_context_t hctx,
				   const string &name, const string &id)
{
  CLS_LOG(20, "dir_remove_image_helper name=%s id=%s",
	  name.c_str(), id.c_str());

  string stored_name, stored_id;
  string name_key = dir_key_for_name(name);
  string id_key = dir_key_for_id(id);
  int r = read_key(hctx, name_key, &stored_id);
  if (r < 0) {
    if (r != -ENOENT)
      CLS_ERR("error reading name to id mapping: %d", r);
    return r;
  }
  r = read_key(hctx, id_key, &stored_name);
  if (r < 0) {
    CLS_ERR("error reading id to name mapping: %d", r);
    return r;
  }

  // check if this op raced with a rename
  if (stored_name != name || stored_id != id) {
    CLS_ERR("stored name '%s' and id '%s' do not match args '%s' and '%s'",
	    stored_name.c_str(), stored_id.c_str(), name.c_str(), id.c_str());
    return -ESTALE;
  }

  r = cls_cxx_map_remove_key(hctx, name_key);
  if (r < 0) {
    CLS_ERR("error removing name: %d", r);
    return r;
  }

  r = cls_cxx_map_remove_key(hctx, id_key);
  if (r < 0) {
    CLS_ERR("error removing id: %d", r);
    return r;
  }

  return 0;
}

/**
 * Rename an image in the directory, updating both indexes
 * atomically. This can't be done from the client calling
 * dir_add_image and dir_remove_image in one transaction because the
 * results of the first method are not visibale to later steps.
 *
 * Input:
 * @param src original name of the image
 * @param dest new name of the image
 * @param id the id of the image
 *
 * Output:
 * @returns -ESTALE if src and id do not map to each other
 * @returns -ENOENT if src or id are not in the directory
 * @returns -EEXIST if dest already exists
 * @returns 0 on success, negative error code on failure
 */
int dir_rename_image(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  string src, dest, id;
  try {
    bufferlist::iterator iter = in->begin();
    ::decode(src, iter);
    ::decode(dest, iter);
    ::decode(id, iter);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  int r = dir_remove_image_helper(hctx, src, id);
  if (r < 0)
    return r;
  // ignore duplicate id because the result of
  // remove_image_helper is not visible yet
  return dir_add_image_helper(hctx, dest, id, false);
}

/**
 * Get the id of an image given its name.
 *
 * Input:
 * @param name the name of the image
 *
 * Output:
 * @param id the id of the image
 * @returns 0 on success, negative error code on failure
 */
int dir_get_id(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  string name;

  try {
    bufferlist::iterator iter = in->begin();
    ::decode(name, iter);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  CLS_LOG(20, "dir_get_id: name=%s", name.c_str());

  string id;
  int r = read_key(hctx, dir_key_for_name(name), &id);
  if (r < 0) {
    if (r != -ENOENT)
      CLS_ERR("error reading id for name '%s': %d", name.c_str(), r);
    return r;
  }
  ::encode(id, *out);
  return 0;
}

/**
 * Get the name of an image given its id.
 *
 * Input:
 * @param id the id of the image
 *
 * Output:
 * @param name the name of the image
 * @returns 0 on success, negative error code on failure
 */
int dir_get_name(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  string id;

  try {
    bufferlist::iterator iter = in->begin();
    ::decode(id, iter);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  CLS_LOG(20, "dir_get_name: id=%s", id.c_str());

  string name;
  int r = read_key(hctx, dir_key_for_id(id), &name);
  if (r < 0) {
    CLS_ERR("error reading name for id '%s': %d", id.c_str(), r);
    return r;
  }
  ::encode(name, *out);
  return 0;
}

/**
 * List the names and ids of the images in the directory, sorted by
 * name.
 *
 * Input:
 * @param start_after which name to begin listing after
 *        (use the empty string to start at the beginning)
 * @param max_return the maximum number of names to list
 *
 * Output:
 * @param images map from name to id of up to max_return images
 * @returns 0 on success, negative error code on failure
 */
int dir_list(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  string start_after;
  uint64_t max_return;

  try {
    bufferlist::iterator iter = in->begin();
    ::decode(start_after, iter);
    ::decode(max_return, iter);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  int max_read = RBD_MAX_KEYS_READ;
  int r = max_read;
  map<string, string> images;
  string last_read = dir_key_for_name(start_after);

  while (r == max_read && images.size() < max_return) {
    map<string, bufferlist> vals;
    CLS_LOG(20, "last_read = '%s'", last_read.c_str());
    r = cls_cxx_map_get_vals(hctx, last_read, RBD_DIR_NAME_KEY_PREFIX,
			     max_read, &vals);
    if (r < 0) {
      CLS_ERR("error reading directory by name: %d", r);
      return r;
    }

    for (map<string, bufferlist>::iterator it = vals.begin();
	 it != vals.end(); ++it) {
      string id;
      bufferlist::iterator iter = it->second.begin();
      try {
	::decode(id, iter);
      } catch (const buffer::error &err) {
	CLS_ERR("could not decode id of image '%s'", it->first.c_str());
	return -EIO;
      }
      CLS_LOG(20, "adding '%s' -> '%s'", dir_name_from_key(it->first).c_str(), id.c_str());
      images[dir_name_from_key(it->first)] = id;
      if (images.size() >= max_return)
	break;
    }
    if (!vals.empty()) {
      last_read = dir_key_for_name(images.rbegin()->first);
    }
  }

  ::encode(images, *out);

  return 0;
}

/**
 * Add an image to the rbd directory. Creates the directory object if
 * needed, and updates the index from id to name and name to id.
 *
 * Input:
 * @param name the name of the image
 * @param id the id of the image
 *
 * Output:
 * @returns -EEXIST if the image name is already in the directory
 * @returns -EBADF if the image id is already in the directory
 * @returns 0 on success, negative error code on failure
 */
int dir_add_image(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  int r = cls_cxx_create(hctx, false);
  if (r < 0) {
    CLS_ERR("could not create directory: error %d", r);
    return r;
  }

  string name, id;
  try {
    bufferlist::iterator iter = in->begin();
    ::decode(name, iter);
    ::decode(id, iter);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  return dir_add_image_helper(hctx, name, id, true);
}

/**
 * Remove an image from the rbd directory.
 *
 * Input:
 * @param name the name of the image
 * @param id the id of the image
 *
 * Output:
 * @returns -ESTALE if the name and id do not map to each other
 * @returns 0 on success, negative error code on failure
 */
int dir_remove_image(cls_method_context_t hctx, bufferlist *in,
		     bufferlist *out)
{
  string name, id;
  try {
    bufferlist::iterator iter = in->begin();
    ::decode(name, iter);
    ::decode(id, iter);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  return dir_remove_image_helper(hctx, name, id);
}


void __cls_init()
{
  CLS_LOG(20, "Loaded rbd class!");

  cls_register("rbd", &h_class);
  cls_register_cxx_method(h_class, "create",
			  CLS_METHOD_RD | CLS_METHOD_WR,
			  create, &h_create);
  cls_register_cxx_method(h_class, "get_features",
			  CLS_METHOD_RD,
			  get_features, &h_get_features);
  cls_register_cxx_method(h_class, "get_size",
			  CLS_METHOD_RD,
			  get_size, &h_get_size);
  cls_register_cxx_method(h_class, "set_size",
			  CLS_METHOD_RD | CLS_METHOD_WR,
			  set_size, &h_set_size);
  cls_register_cxx_method(h_class, "get_object_prefix",
			  CLS_METHOD_RD,
			  get_object_prefix, &h_get_object_prefix);
  cls_register_cxx_method(h_class, "get_all_features",
			  CLS_METHOD_RD,
			  get_all_features, &h_get_all_features);
  cls_register_cxx_method(h_class, "get_stripe_unit_count",
			  CLS_METHOD_RD,
			  get_stripe_unit_count, &h_get_stripe_unit_count);
  cls_register_cxx_method(h_class, "set_stripe_unit_count",
			  CLS_METHOD_RD | CLS_METHOD_WR,
			  set_stripe_unit_count, &h_set_stripe_unit_count);


  /* methods for the rbd_id.$image_name objects */
  cls_register_cxx_method(h_class, "get_id",
			  CLS_METHOD_RD,
			  get_id, &h_get_id);
  cls_register_cxx_method(h_class, "set_id",
			  CLS_METHOD_RD | CLS_METHOD_WR,
			  set_id, &h_set_id);

  /* methods for the rbd_directory object */
  cls_register_cxx_method(h_class, "dir_get_id",
			  CLS_METHOD_RD,
			  dir_get_id, &h_dir_get_id);
  cls_register_cxx_method(h_class, "dir_get_name",
			  CLS_METHOD_RD,
			  dir_get_name, &h_dir_get_name);
  cls_register_cxx_method(h_class, "dir_list",
			  CLS_METHOD_RD,
			  dir_list, &h_dir_list);
  cls_register_cxx_method(h_class, "dir_add_image",
			  CLS_METHOD_RD | CLS_METHOD_WR,
			  dir_add_image, &h_dir_add_image);
  cls_register_cxx_method(h_class, "dir_remove_image",
			  CLS_METHOD_RD | CLS_METHOD_WR,
			  dir_remove_image, &h_dir_remove_image);
  cls_register_cxx_method(h_class, "dir_rename_image",
			  CLS_METHOD_RD | CLS_METHOD_WR,
			  dir_rename_image, &h_dir_rename_image);

  return;
}
