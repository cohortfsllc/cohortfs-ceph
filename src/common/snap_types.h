#ifndef __CEPH_SNAP_TYPES_H
#define __CEPH_SNAP_TYPES_H

#include "include/types.h"

namespace ceph {

class Formatter;
}
struct SnapContext {
  snapid_t seq;            // 'time' stamp
  vector<snapid_t> snaps;  // existent snaps, in descending order

  SnapContext() {}
  SnapContext(snapid_t s, const vector<snapid_t>& v) : seq(s), snaps(v) {}    

  bool is_valid() const;

  void clear() {
    seq = 0;
    snaps.clear();
  }
  bool empty() { return seq == 0; }

  void encode(bufferlist& bl) const {
    ::encode(seq, bl);
    ::encode(snaps, bl);
  }
  void decode(bufferlist::iterator& bl) {
    ::decode(seq, bl);
    ::decode(snaps, bl);
  }
  void dump(Formatter *f) const;
  static void generate_test_instances(list<SnapContext*>& o);
};
WRITE_CLASS_ENCODER(SnapContext)

inline ostream& operator<<(ostream& out, const SnapContext& snapc) {
  return out << snapc.seq << "=" << snapc.snaps;
}

//}

#endif
