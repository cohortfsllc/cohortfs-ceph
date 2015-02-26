
#include "cls/version/cls_version_types.h"
#include "common/Formatter.h"
#include "common/ceph_json.h"


void obj_version::dump(Formatter *f) const
{
  f->dump_int("ver", ver);
  f->dump_string("tag", tag);
}

void obj_version::decode_json(JSONObj *oid)
{
  JSONDecoder::decode_json("ver", ver, oid);
  JSONDecoder::decode_json("tag", tag, oid);
}

