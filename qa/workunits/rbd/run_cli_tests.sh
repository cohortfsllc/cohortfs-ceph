#!/bin/bash

wget -q http://ceph.com/qa/rbd_cli_tests.pls
wget -q http://ceph.com/qa/RbdLib.pm
perl rbd_cli_tests.pls --pool test
exit 0

