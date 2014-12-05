@0xe71374404433a0c6;

using Cxx = import "/capnp/c++.capnp";
using Ceph = import "ceph-common.capnp";

$Cxx.namespace("Test");

struct Foo {
    maxOsd        @0 :Int32;
    epoch         @1 :Ceph.Epoch;
    created       @2 :Ceph.UTime;
    fsid          @3 :Ceph.Uuid;
}
