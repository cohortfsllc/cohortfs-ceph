#!/bin/sh

capnp compile -oc++ test-small.capnp
g++ -std=c++11 -I../.. -I../../../build/include -o test-small test-small.cc -lkj -lcapnp 
