@0xafd07975e4b248c8;
using Cxx = import "/capnp/c++.capnp";
$Cxx.namespace("Captain");

struct OSDMap {
    structV       @0 :UInt8;
    structCompat  @1 :UInt8;
    structLen     @2 :UInt32;
    fsid          @3 :Uuid;
    epoch         @4 :Epoch;
    created       @5 :UTime;
    modified      @6 :UTime;
    flags         @7 :UInt32;
    maxOsd        @8 :Int32;
    osdState      @9 :List(UInt8);
    osdWeight    @10 :List(UInt32);
    volumes      @11 :List(Volume);
    hbBackAddr   @12 :List(EntityAddr);
    osdInfo      @13 :List(OsdInfo);
    blacklist    @14 :List(EntityAddrUTimePair);
    clusterAddr  @15 :List(EntityAddr);
    osdUuid      @16 :List(Uuid);
    osdXInfo     @17 :List(OsdXInfo);
    hbFrontAddr  @18 :List(EntityAddr);
}

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

struct Volume {
  version @0: Int32;
  type @1: VolType;
  id @2: Uuid;
  name @3: Text;
  lastUpdate @4: Epoch;

  enum VolType {
    cohortVol @0;
    notAVolType @1;
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

# from OSDMap.h
struct OsdInfo {
   upFrom @0 :Epoch;
   upThru @1 :Epoch;
   downAt @2 :Epoch;
}

# facilitate blacklist
struct EntityAddrUTimePair {
    entityAddr @0 :EntityAddr;
    time       @1 :UTime;
}

struct OsdXInfo {
    downStamp        @0 :UTime;
    laggyProbability @1 :UInt32;
    laggyInterval    @2 :UInt32;
    features         @3 :UInt64;
}
