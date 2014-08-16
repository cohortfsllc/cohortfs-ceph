#!/bin/bash
#
# Copyright (C) 2013,2014 Cloudwatt <libre.licensing@cloudwatt.com>
#
# Author: Loic Dachary <loic@dachary.org>
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Library Public License as published by
# the Free Software Foundation; either version 2, or (at your option)
# any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Library Public License for more details.
#
source test/mon/mon-test-helpers.sh

function run() {
    local dir=$1

    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth-supported=none "
    CEPH_ARGS+="--mon-host=127.0.0.1 "

    FUNCTIONS=${FUNCTIONS:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for TEST_function in $FUNCTIONS ; do
        setup $dir || return 1
        $TEST_function $dir || return 1
        teardown $dir || return 1
    done
}

function TEST_default_deprectated_0() {
    local dir=$1
    # explicitly set the default crush rule
    expected=66
    run_mon $dir a --public-addr 127.0.0.1 \
        --osd_pool_default_crush_replicated_ruleset $expected
    ./ceph --format json osd dump | grep '"crush_ruleset":'$expected
    ! grep "osd_pool_default_crush_rule is deprecated " $dir/a/log || return 1
}

function TEST_default_deprectated_1() {
    local dir=$1
    # explicitly set the default crush rule using deprecated option
    expected=55
    run_mon $dir a --public-addr 127.0.0.1 \
        --osd_pool_default_crush_rule $expected
    ./ceph --format json osd dump | grep '"crush_ruleset":'$expected
    grep "osd_pool_default_crush_rule is deprecated " $dir/a/log || return 1
}

function TEST_default_deprectated_2() {
    local dir=$1
    expected=77
    unexpected=33
    run_mon $dir a --public-addr 127.0.0.1 \
        --osd_pool_default_crush_rule $expected \
        --osd_pool_default_crush_replicated_ruleset $unexpected
    ./ceph --format json osd dump | grep '"crush_ruleset":'$expected
    ! ./ceph --format json osd dump | grep '"crush_ruleset":'$unexpected || return 1
    grep "osd_pool_default_crush_rule is deprecated " $dir/a/log || return 1
}

function TEST_erasure_crush_rule() {
    local dir=$1
    run_mon $dir a --public-addr 127.0.0.1
    #
    # choose the crush ruleset used with an erasure coded pool
    #
    local crush_ruleset=myruleset
    ! ./ceph osd crush rule ls | grep $crush_ruleset || return 1
    ./ceph osd crush rule create-erasure $crush_ruleset
    ./ceph osd crush rule ls | grep $crush_ruleset
    local poolname
    poolname=pool_erasure1
    ! ./ceph --format json osd dump | grep '"crush_ruleset":1' || return 1
    ./ceph osd pool create $poolname 12 12 erasure default $crush_ruleset
    ./ceph --format json osd dump | grep '"crush_ruleset":1' || return 1
    #
    # a crush ruleset by the name of the pool is implicitly created
    #
    poolname=pool_erasure2
    ./ceph osd erasure-code-profile set myprofile
    ./ceph osd pool create $poolname 12 12 erasure myprofile
    ./ceph osd crush rule ls | grep $poolname || return 1
}

function TEST_erasure_crush_rule_pending() {
    local dir=$1
    run_mon $dir a --public-addr 127.0.0.1
    # try again if the ruleset creation is pending
    crush_ruleset=erasure_ruleset
    # add to the pending OSD map without triggering a paxos proposal
    result=$(echo '{"prefix":"osdmonitor_prepare_command","prepare":"osd crush rule create-erasure","name":"'$crush_ruleset'"}' | nc -U $dir/a/ceph-mon.a.asok | cut --bytes=5-)
    test $result = true || return 1
    ./ceph osd pool create pool_erasure 12 12 erasure default $crush_ruleset || return 1
    grep "$crush_ruleset try again" $dir/a/log || return 1
}

function TEST_erasure_crush_stripe_width() {
    local dir=$1
    # the default stripe width is used to initialize the pool
    run_mon $dir a --public-addr 127.0.0.1
    stripe_width=$(./ceph-conf --show-config-value osd_pool_erasure_code_stripe_width)
    ./ceph osd pool create pool_erasure 12 12 erasure
    ./ceph --format json osd dump | tee $dir/osd.json
    grep '"stripe_width":'$stripe_width $dir/osd.json > /dev/null || return 1
}

function TEST_erasure_crush_stripe_width_padded() {
    local dir=$1
    # setting osd_pool_erasure_code_stripe_width modifies the stripe_width
    # and it is padded as required by the default plugin
    profile+=" plugin=jerasure"
    profile+=" technique=reed_sol_van"
    k=4
    profile+=" k=$k"
    profile+=" m=2"
    expected_chunk_size=2048
    actual_stripe_width=$(($expected_chunk_size * $k))
    desired_stripe_width=$(($actual_stripe_width - 1))
    run_mon $dir a --public-addr 127.0.0.1 \
        --osd_pool_erasure_code_stripe_width $desired_stripe_width \
        --osd_pool_default_erasure_code_profile "$profile"
    ./ceph osd pool create pool_erasure 12 12 erasure
    ./ceph osd dump | tee $dir/osd.json
    grep "stripe_width $actual_stripe_width" $dir/osd.json > /dev/null || return 1
}

function TEST_erasure_code_pool() {
    local dir=$1
    run_mon $dir a --public-addr 127.0.0.1
    ./ceph --format json osd dump > $dir/osd.json
    local expected='"erasure_code_profile":"default"'
    ! grep "$expected" $dir/osd.json || return 1
    ./ceph osd pool create erasurecodes 12 12 erasure
    ./ceph --format json osd dump | tee $dir/osd.json
    grep "$expected" $dir/osd.json > /dev/null || return 1

    ./ceph osd pool create erasurecodes 12 12 erasure 2>&1 | \
        grep 'already exists' || return 1
    ./ceph osd pool create erasurecodes 12 12 2>&1 | \
        grep 'cannot change to type replicated' || return 1
}

function TEST_replicated_pool() {
    local dir=$1
    run_mon $dir a --public-addr 127.0.0.1
    ./ceph osd pool create replicated 12 12 replicated
    ./ceph osd pool create replicated 12 12 replicated 2>&1 | \
        grep 'already exists' || return 1
    ./ceph osd pool create replicated 12 12 # default is replicated
    ./ceph osd pool create replicated 12    # default is replicated, pgp_num = pg_num
    ./ceph osd pool create replicated 12 12 erasure 2>&1 | \
        grep 'cannot change to type erasure' || return 1
}

main osd-pool-create

# Local Variables:
# compile-command: "cd ../.. ; make -j4 && test/mon/osd-pool-create.sh"
# End:
