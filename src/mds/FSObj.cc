/* -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- */
// vim: ts=8 sw=2 smarttab

#include "FSObj.h"

using namespace cohort::mds;

FSObj::FSObj(_inodeno_t ino, const identity &who, int type)
  : ino(ino)
{
  attr.filesize = 0;
  attr.mode = 0777;
  attr.user = who.uid;
  attr.group = who.gid;
  attr.atime = attr.mtime = attr.ctime = ceph::real_clock::now();
  attr.nlinks = 1;
  attr.type = type;
  attr.rawdev = 0;
}

static void copy_attrs(int mask, ObjAttr &to, const ObjAttr &from)
{
  if (mask & ATTR_SIZE) to.filesize = from.filesize;
  if (mask & ATTR_MODE) to.mode = from.mode;
  if (mask & ATTR_GROUP) to.group = from.group;
  if (mask & ATTR_OWNER) to.user = from.user;
  if (mask & ATTR_ATIME) to.atime = from.atime;
  if (mask & ATTR_MTIME) to.mtime = from.mtime;
  if (mask & ATTR_CTIME) to.ctime = from.ctime;
  if (mask & ATTR_NLINKS) to.nlinks = from.nlinks;
  if (mask & ATTR_TYPE) to.type = from.type;
  if (mask & ATTR_RAWDEV) to.rawdev = from.rawdev;
}

int FSObj::getattr(int mask, ObjAttr &attrs) const
{
  copy_attrs(mask, attrs, attr);
  return 0;
}

int FSObj::setattr(int mask, const ObjAttr &attrs)
{
  copy_attrs(mask, attr, attrs);
  return 0;
}

int FSObj::lookup(const std::string &name, FSObj **obj) const
{
  if (!is_dir())
    return -ENOTDIR;
  auto i = entries.find(name);
  if (i == entries.end())
    return -ENOENT;
  *obj = i->second;
  return 0;
}

int FSObj::link(const std::string &name, FSObj *obj)
{
  if (!is_dir())
    return -ENOTDIR;
  auto i = entries.insert(std::make_pair(name, obj));
  return i.second ? 0 : -EEXIST;
}

int FSObj::unlink(const std::string &name, FSObj **obj)
{
  if (!is_dir())
    return -ENOTDIR;
  auto i = entries.find(name);
  if (i == entries.end())
    return -ENOENT;
  if (!i->second->is_empty())
    return -ENOTEMPTY;
  *obj = i->second;
  entries.erase(i);
  return 0;
}
