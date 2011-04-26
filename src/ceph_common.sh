#!/bin/sh

CCONF="$BINDIR/cconf"

default_conf=$ETCDIR"/ceph.conf"
conf=$default_conf

hostname=`hostname | cut -d . -f 1`

figure_dirs() {
    if echo $bindir | grep -q \@; then
	echo "using current dir"
	BINDIR=.
	LIBDIR=.
	ETCDIR=.
    else
	echo "all good"
	
    fi
}

verify_conf() {
    # fetch conf?
    if [ -x "$ETCDIR/fetch_config" ] && [ "$conf" = "$default_conf" ]; then
	conf="/tmp/fetched.ceph.conf.$$"
	echo "[$ETCDIR/fetch_config $conf]"
	if $ETCDIR/fetch_config $conf && [ -e $conf ]; then true ; else
	    echo "$0: failed to fetch config with '$ETCDIR/fetch_config $conf'"
	    exit 1
	fi
	# yay!
    else
        # make sure ceph.conf exists
	if [ ! -e $conf ]; then
	    if [ "$conf" = "$default_conf" ]; then
		echo "$0: ceph conf $conf not found; system is not configured."
		exit 0
	    fi
	    echo "$0: ceph conf $conf not found!"
	    usage_exit
	fi
    fi
}


check_host() {
    # what host is this daemon assigned to?
    host=`$CCONF -c $conf -n $type.$id host`
    [ "$host" = "localhost" ] && host=""
    ssh=""
    rootssh=""
    sshdir=$PWD
    get_conf user "" "user"
    if [ -n "$host" ]; then
	#echo host for $name is $host, i am $hostname
	if [ "$host" != "$hostname" ]; then
	    # skip, unless we're starting remote daemons too
	    if [ $allhosts -eq 0 ]; then
		return 1
	    fi

	    # we'll need to ssh into that host
	    if [ -z "$user" ]; then
		ssh="ssh $host"
	    else
		ssh="ssh $user@$host"
	    fi
	    rootssh="ssh root@$host"
	    get_conf sshdir "$sshdir" "ssh path"
	fi
    else
	host=$hostname
    fi

    echo "=== $type.$id === "

    return 0
}

do_cmd() {
    if [ -z "$ssh" ]; then
	[ $verbose -eq 1 ] && echo "--- $host# $1"
	ulimit -c unlimited
	whoami=`whoami`
	if [ "$whoami" = "$user" ] || [ -z "$user" ]; then
	    bash -c "$1" || { [ -z "$3" ] && echo "failed: '$1'" && exit 1; }
	else
	    sudo su $user -c "$1" || { [ -z "$3" ] && echo "failed: '$1'" && exit 1; }
	fi
    else
	[ $verbose -eq 1 ] && echo "--- $ssh $2 \"cd $sshdir ; ulimit -c unlimited ; $1\""
	$ssh $2 "cd $sshdir ; ulimit -c unlimited ; $1" || { [ -z "$3" ] && echo "failed: '$ssh $1'" && exit 1; }
    fi
}

do_root_cmd() {
    if [ -z "$ssh" ]; then
	[ $verbose -eq 1 ] && echo "--- $host# $1"
	ulimit -c unlimited
	whoami=`whoami`
	if [ "$whoami" = "root" ] || [ -z "$user" ]; then
	    bash -c "$1" || { echo "failed: '$1'" ; exit 1; }
	else
	    sudo bash -c "$1" || { echo "failed: '$1'" ; exit 1; }
	fi
    else
	[ $verbose -eq 1 ] && echo "--- $rootssh $2 \"cd $sshdir ; ulimit -c unlimited ; $1\""
	$rootssh $2 "cd $sshdir ; ulimit -c unlimited ; $1" || { echo "failed: '$rootssh $1'" ; exit 1; }
    fi
}

get_name_list() {
    orig=$1

    if [ -z "$orig" ]; then
        # extract list of monitors, mdss, osds defined in startup.conf
	what=`$CCONF -c $conf -l mon | egrep -v '^mon$' ; \
	    $CCONF -c $conf -l mds | egrep -v '^mds$' ; \
	    $CCONF -c $conf -l osd | egrep -v '^osd$'`
	return
    fi

    what=""
    for f in $orig; do
	type=`echo $f | cut -c 1-3`   # e.g. 'mon', if $item is 'mon1'
	id=`echo $f | cut -c 4- | sed 's/\\.//'`
	all=`$CCONF -c $conf -l $type | egrep -v "^$type$" || true`
	case $f in
	    mon | osd | mds)
		what="$what $all"
		;;
	    *)
		if echo " " $all " " | egrep -v -q "( $type$id | $type.$id )"; then
		    echo "$0: $type.$id not found ($conf defines \"$all\")"
		    exit 1
		fi
		what="$what $f"
		;;
	esac
    done
}

get_conf() {
	var=$1
	def=$2
	key=$3
	shift; shift; shift

	if [ -z "$1" ]; then
	    [ "$verbose" -eq 1 ] && echo "$CCONF -c $conf -n $type.$id \"$key\""
	    eval "$var=\"`$CCONF -c $conf -n $type.$id \"$key\" || eval echo -n \"$def\"`\""
	else
	    [ "$verbose" -eq 1 ] && echo "$CCONF -c $conf -s $1 \"$key\""
	    eval "$var=\"`$CCONF -c $conf -s $1 \"$key\" || eval echo -n \"$def\"`\""
	fi
}

get_conf_bool() {
	get_conf "$@"

	eval "val=$"$1
	[ "$val" = "0" ] && export $1=0
	[ "$val" = "false" ] && export $1=0
	[ "$val" = "1" ] && export $1=1
	[ "$val" = "true" ] && export $1=1
}

