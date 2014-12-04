@0xe67e4e25618791ab;

using Cxx = import "/capnp/c++.capnp";

$Cxx.namespace("Captain");

struct Uuid {
   uuid @0: Data;
}

struct Epoch {
   epoch @0: UInt32;
}

struct UTime {
  tv :group {
    tvSec @0: UInt32;
    tvNsec @1: UInt32;
  }
}

# from msg/msg_types.h
struct EntityAddr {
  type @0: UInt32;
  nonce @1: UInt32;
  addr @2: Data; # raw encoding
}

# from netinet/in.h
struct SockAddrStorage {
   data @0 :Data;
}

# facilitate OSD blacklist
struct EntityAddrUTimePair {
    entityAddr @0 :EntityAddr;
    time       @1 :UTime;
}
