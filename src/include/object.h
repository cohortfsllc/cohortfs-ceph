// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#ifndef CEPH_OBJECT_H
#define CEPH_OBJECT_H

#include <stdio.h>
#include <inttypes.h>

#include <iostream>
#include <iomanip>
using namespace std;

#include <ext/hash_map>
using namespace __gnu_cxx;

#include "hash.h"
#include "encoding.h"
#include "ceph_hash.h"
#include "cmp.h"
#include "common/armor.h"

/// Maximum supported object name length for Ceph, in bytes.
#define MAX_CEPH_OBJECT_NAME_LEN 4096

static const uuid_d OSD_PSEUDO_VOLUME = uuid_d();
static const uuid_d INVALID_VOLUME = uuid_d(-1, -1);

struct object_t {
private:

  void store(size_t len, const char* data) {
    if (id) {
      delete[] id;
      id = 0;
    }

    idsize = len;
    if (idsize == 0) {
      id = NULL;
    } else {
      id = new char[idsize];
      memcpy((void *) id, data, idsize);
    }
  }

public:
  uuid_d volume;
  size_t idsize;
  const char *id;

  object_t() : volume(INVALID_VOLUME), idsize(0), id(NULL) {
  }

  object_t(const object_t& o)
    : volume(o.volume), idsize(0), id(NULL) {
    store(o.idsize, o.id);
  }

  object_t(const uuid_d& v, const size_t len, const char *s)
    : volume(v), idsize(0), id(NULL) {
    store(len, s);
  }
  object_t(const uuid_d& v, const string s)
    : volume(v), idsize(0), id(NULL) {
    store(s.size(), s.data());
  }
  object_t(const uuid_d& v, const char *s)
    : volume(v), idsize(0), id(NULL) {
    store(strlen(s), s);
  }
  /* These constructors suck and exist for now, but will be removed
     when we have more time to update the librados interface/structure. */
  object_t(const char *s)
    : volume(INVALID_VOLUME), idsize(0), id(NULL) {
    store(strlen(s), s);
  }
  object_t(const string s)
    : volume(INVALID_VOLUME), idsize(0), id(NULL) {
    store(s.size(), s.data());
  }
  ~object_t(void) {
    if (id) {
      delete[] id;
    }
  }

  void clear() {
    volume.clear();
    idsize = 0;
    if (id) {
      delete id;
      id = NULL;
    }
  }

  void encode(bufferlist &bl) const {
    ::encode(volume, bl);
    ::encode(idsize, bl);
    bl.append(id, idsize);
  }
  void decode(bufferlist::iterator &bl) {
    ::decode(volume, bl);
    ::decode(idsize, bl);
    if (id) {
      delete id;
    }
    id = new char[idsize];
    bl.copy(idsize, (char *)id);
  }

  string to_str() const {
    size_t n = idsize * 4 / 3 + 3;
    char b[n];
    string s(volume);
    s.append("_");
    size_t l = ceph_armor(b, b+n, id, id + idsize);
    s.append(b, l);
    return s;
  }

  object_t& operator=(const object_t &o) {
    volume = o.volume;
    store(o.idsize, o.id);
    return *this;
  }
};
WRITE_CLASS_ENCODER(object_t)

inline bool operator==(const object_t& l, const object_t& r) {
  return ((l.volume == r.volume) &&
	  (memcmp(l.id, r.id, min(l.idsize, r.idsize)) == 0));
}
inline bool operator!=(const object_t& l, const object_t& r) {
    return ((l.volume != r.volume) ||
	    (memcmp(l.id, r.id, min(l.idsize, r.idsize)) != 0));
}
inline bool operator>(const object_t& l, const object_t& r) {
  if (l.volume > r.volume) {
    return true;
  } else if (l.volume < r.volume) {
    return false;
  } else {
    return memcmp(l.id, r.id, min(l.idsize, r.idsize) > 0);
  }
}
inline bool operator<(const object_t& l, const object_t& r) {
  if (l.volume < r.volume) {
    return true;
  } else if (l.volume > r.volume) {
    return false;
  } else {
    return memcmp(l.id, r.id, min(l.idsize, r.idsize)) < 0;
  }
}
inline bool operator>=(const object_t& l, const object_t& r) {
  if (l.volume > r.volume) {
    return true;
  } else if (l.volume < r.volume) {
    return false;
  } else {
    return memcmp(l.id, r.id, min(l.idsize, r.idsize)) >= 0;
  }
}

inline bool operator<=(const object_t& l, const object_t& r) {
  if (l.volume < r.volume) {
    return true;
  } else if (l.volume > r.volume) {
    return false;
  } else {
    return memcmp(l.id, r.id, min(l.idsize, r.idsize)) <= 0;
  }
}
inline ostream& operator<<(ostream& out, const object_t& o) {
  return out << o.to_str();
}

namespace __gnu_cxx {
  template<> struct hash<object_t> {
    size_t operator()(const object_t& r) const {
      return ceph_str_hash_rjenkins(r.id, r.idsize);
    }
  };
}

// ---------------------------
// snaps

struct snapid_t {
  uint64_t val;
  snapid_t(uint64_t v=0) : val(v) {}
  snapid_t operator+=(snapid_t o) { val += o.val; return *this; }
  snapid_t operator++() { ++val; return *this; }
  operator uint64_t() const { return val; }
};

inline void encode(snapid_t i, bufferlist &bl) { encode(i.val, bl); }
inline void decode(snapid_t &i, bufferlist::iterator &p) { decode(i.val, p); }

inline ostream& operator<<(ostream& out, snapid_t s) {
  if (s == CEPH_NOSNAP)
    return out << "head";
  else if (s == CEPH_SNAPDIR)
    return out << "snapdir";
  else
    return out << hex << s.val << dec;
}


struct sobject_t {
  object_t oid;
  snapid_t snap;

  sobject_t() : snap(0) {}
  sobject_t(object_t o, snapid_t s) : oid(o), snap(s) {}

  void encode(bufferlist& bl) const {
    ::encode(oid, bl);
    ::encode(snap, bl);
  }
  void decode(bufferlist::iterator& bl) {
    ::decode(oid, bl);
    ::decode(snap, bl);
  }
};
WRITE_CLASS_ENCODER(sobject_t)

inline bool operator==(const sobject_t &l, const sobject_t &r) {
  return l.oid == r.oid && l.snap == r.snap;
}
inline bool operator!=(const sobject_t &l, const sobject_t &r) {
  return l.oid != r.oid || l.snap != r.snap;
}
inline bool operator>(const sobject_t &l, const sobject_t &r) {
  return l.oid > r.oid || (l.oid == r.oid && l.snap > r.snap);
}
inline bool operator<(const sobject_t &l, const sobject_t &r) {
  return l.oid < r.oid || (l.oid == r.oid && l.snap < r.snap);
}
inline bool operator>=(const sobject_t &l, const sobject_t &r) {
  return l.oid > r.oid || (l.oid == r.oid && l.snap >= r.snap);
}
inline bool operator<=(const sobject_t &l, const sobject_t &r) {
  return l.oid < r.oid || (l.oid == r.oid && l.snap <= r.snap);
}
inline ostream& operator<<(ostream& out, const sobject_t &o) {
  return out << o.oid << "/" << o.snap;
}
namespace __gnu_cxx {
  template<> struct hash<sobject_t> {
    size_t operator()(const sobject_t &r) const {
      static hash<object_t> H;
      static rjhash<uint64_t> I;
      return H(r.oid) ^ I(r.snap);
    }
  };
}

#endif
