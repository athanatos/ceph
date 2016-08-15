#!/bin/bash
#
# Copyright (C) 2013 Cloudwatt <libre.licensing@cloudwatt.com>
# Copyright (C) 2015 Red Hat <contact@redhat.com>
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

source $CEPH_ROOT/qa/workunits/ceph-helpers.sh

export CEPH_VSTART_WRAPPER=1
export CEPH_DIR="${TMPDIR:-$PWD}/testdir/test-$CEPH_PORT"
export CEPH_DEV_DIR="$CEPH_DIR/dev"
export CEPH_OUT_DIR="$CEPH_DIR/out"

function vstart_setup()
{
    rm -fr $CEPH_DEV_DIR $CEPH_OUT_DIR
    mkdir -p $CEPH_DEV_DIR

    if [ -z ${VSTART_WRAPPER_SKIP_TEARDOWN+x} ]
    then
      trap "teardown $CEPH_DIR" EXIT
    fi

    export LC_ALL=C # some tests are vulnerable to i18n
    export PATH="$(pwd):${PATH}"
    $CEPH_ROOT/src/vstart.sh \
        --short \
        -o 'paxos propose interval = 0.01' \
        -d -e -n -l $CEPH_START || return 1
    export CEPH_CONF=$CEPH_DIR/ceph.conf

    crit=$(expr 100 - $(ceph-conf --show-config-value mon_data_avail_crit))
    if [ $(df . | perl -ne 'print if(s/.*\s(\d+)%.*/\1/)') -ge $crit ] ; then
        df . 
        cat <<EOF
error: not enough free disk space for mon to run
The mon will shutdown with a message such as 
 "reached critical levels of available space on local monitor storage -- shutdown!"
as soon as it finds the disk has is more than ${crit}% full. 
This is a limit determined by
 ceph-conf --show-config-value mon_data_avail_crit
EOF
        return 1
    fi


    if [ -n ${CREATE_REP_CACHE+x} ]
    then
        ceph osd pool create rep-base 4
        ceph osd pool create rep-cache 4
        ceph osd tier add rep-base rep-cache
        ceph osd tier cache-mode rep-cache writeback
        ceph osd tier set-overlay rep-base rep-cache
        ceph osd pool set rep-cache hit_set_type bloom
        ceph osd pool set rep-cache hit_set_count 8
        ceph osd pool set rep-cache hit_set_period 3600
        ceph osd pool set rep-cache target_max_objects 250
        ceph osd pool set rep-cache min_read_recency_for_promote 2
    fi
}

function main()
{
    teardown $CEPH_DIR
    vstart_setup || return 1
    if [ -n ${VSTART_WRAPPER_SKIP_TEARDOWN+x} ]
    then
      CEPH_CONF=$CEPH_DIR/ceph.conf "$@" || return 1
    else
      ( CEPH_CONF=$CEPH_DIR/ceph.conf "$@" && teardown $CEPH_DIR ) || return 1
    fi
}

main "$@"
