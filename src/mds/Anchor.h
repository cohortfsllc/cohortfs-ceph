 /** modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.	See file COPYING.
 *
 */

#ifndef CEPH_ANCHOR_H
#define CEPH_ANCHOR_H

#include <string>
using std::string;

#include "include/types.h"
#include "mdstypes.h"
#include "include/buffer.h"


// identifies a anchor table mutation

namespace ceph {
  class Formatter;
}

// anchor type

class Anchor {
public:
  inodeno_t ino;      // anchored ino
  inodeno_t dirino;
  uint32_t     dn_hash;
  int	    nref;     // reference count
  version_t updated;

  Anchor() : dn_hash(0), nref(0), updated(0) {}
  Anchor(inodeno_t i, inodeno_t di, uint32_t hash, int nr, version_t u) :
    ino(i), dirino(di), dn_hash(hash), nref(nr), updated(u) { }

  void encode(bufferlist &bl) const;
  void decode(bufferlist::iterator &bl);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<Anchor*>& ls);
};
WRITE_CLASS_ENCODER(Anchor)

template <typename T>
typename StrmRet<T>::type& operator<<(T& out, const Anchor &a)
{
  return out << "a(" << a.ino << " " << a.dirino << "/" << a.dn_hash << " " << a.nref << " v" << a.updated << ")";
}

#endif
