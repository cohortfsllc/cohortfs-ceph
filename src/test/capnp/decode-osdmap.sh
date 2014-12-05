#!/bin/sh

cat /tmp/osdmap.message | capnp decode OSDMap.capnp OSDMap
