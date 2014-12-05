@0xafd07975e4b248c8;

using Cxx = import "/capnp/c++.capnp";
using Ceph = import "ceph-common.capnp";

$Cxx.namespace("Captain");

struct OSDMap {
    maxOsd        @0 :Int32;
    fsid          @1 :Ceph.Uuid;
    epoch         @2 :Ceph.Epoch;
    created       @3 :Ceph.UTime;
    modified      @4 :Ceph.UTime;
    flags         @5 :UInt32;
#    structV       @6 :UInt8;
#    structCompat  @7 :UInt8;
#    structLen     @8 :UInt32;
#    osdState      @9 :List(UInt8);
#    osdWeight    @10 :List(UInt32);
#    volumes      @11 :List(Volume);
#    hbBackAddr   @12 :List(Ceph.EntityAddr);
#    osdInfo      @13 :List(OsdInfo);
#    blacklist    @14 :List(Ceph.EntityAddrUTimePair);
#    clusterAddr  @15 :List(Ceph.EntityAddr);
#    osdUuid      @16 :List(Ceph.Uuid);
#    osdXInfo     @17 :List(OsdXInfo);
#    hbFrontAddr  @18 :List(Ceph.EntityAddr);
}

struct Volume {
  version    @0 :Int32;
  type       @1 :VolType;
  id         @2 :Ceph.Uuid;
  name       @3 :Text;
  lastUpdate @4 :Ceph.Epoch;

  enum VolType {
    cohortVol   @0;
    notAVolType @1;
  }
}

# from OSDMap.h
struct OsdInfo {
   upFrom @0 :Ceph.Epoch;
   upThru @1 :Ceph.Epoch;
   downAt @2 :Ceph.Epoch;
}

struct OsdXInfo {
    downStamp        @0 :Ceph.UTime;
    laggyProbability @1 :UInt32;
    laggyInterval    @2 :UInt32;
    features         @3 :UInt64;
}
