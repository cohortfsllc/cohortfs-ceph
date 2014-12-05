#!/bin/sh

cat /tmp/test-small.message | capnp decode test-small.capnp Foo
