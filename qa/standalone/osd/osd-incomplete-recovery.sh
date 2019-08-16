#!/usr/bin/env bash
#
# Copyright (C) 2019 Red Hat <contact@redhat.com>
#
# Author: Samuel Just <sjust@redhat.com>
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

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh

function run() {
    local dir=$1
    shift

    # Fix port????
    export CEPH_MON="127.0.0.1:7129" # git grep '\<7129\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth-supported=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON --osd_max_backfills=1 --debug_reserver=20 "

    local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        setup $dir || return 1
        $func $dir || return 1
        teardown $dir || return 1
    done
}


function TEST_incomplete_recovery() {
    local dir=$1
    local extra_opts=""
    local objects=20

    local OSDS=4

    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    export CEPH_ARGS
    export EXTRA_OPTS=" $extra_opts"

    for osd in $(seq 0 $(expr $OSDS - 1))
    do
      run_osd $dir $osd || return 1
      ceph tell osd.$osd config set osd_debug_no_purge_strays true
      ceph osd crush add osd.$i 1 host=host$i
      ceph osd crush move host$i root=default
      ceph osd primary-affinity osd.$i 0
      wait_for_osd up osd.$i || return 1
    done
    ceph osd crush move osd.3 host=host0
    ceph osd crush reweight osd.0 0
    ceph osd primary-affinity osd.0 1
    ceph osd primary-affinity osd.3 1

    ceph osd erasure-code-profile set testprofile \
	 plugin=jerasure \
	 k=2 \
	 m=1 \
	 technique=reed_sol_van \
	 ruleset-root=default \
	 ruleset-failure-domain=host
	 
    ceph osd crush rule create-erasure testrule testprofile 

    create_pool test 1 1 erasure testprofile testrule 20

    for j in $(seq 1 $objects)
    do
       rados -p test put obj-${j} /etc/passwd
    done

    ceph osd crush reweight osd.0 1
    ceph osd crush reweight osd.3 0

    wait_for_clean || return 1

    kill_daemons $dir TERM osd.2 || return 1
    ceph tell osd.0 config set osd_debug_no_purge_strays false

    sleep 20

    activate_osd $dir 2 || return 1

    for osd in $(seq 0 $(expr $OSDS - 1))
    do
	wait_for_osd up $i || return 1
    done
}

# Local Variables:
# compile-command: "make -j4 && ../qa/run-standalone.sh osd-backfill-recovery-log.sh"
# End:
