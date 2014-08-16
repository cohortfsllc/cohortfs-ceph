#!/bin/sh

export PYTHONPATH=./pybind
export LD_LIBRARY_PATH=.libs
export DYLD_LIBRARY_PATH=$LD_LIBRARY_PATH


# abort on failure
set -e

[ -z "$CEPH_NUM_MON" ] && CEPH_NUM_MON="$MON"
[ -z "$CEPH_NUM_OSD" ] && CEPH_NUM_OSD="$OSD"
[ -z "$CEPH_NUM_MDS" ] && CEPH_NUM_MDS="$MDS"
[ -z "$CEPH_NUM_RGW" ] && CEPH_NUM_RGW="$RGW"

[ -z "$CEPH_NUM_MON" ] && CEPH_NUM_MON=3
[ -z "$CEPH_NUM_OSD" ] && CEPH_NUM_OSD=1
[ -z "$CEPH_NUM_MDS" ] && CEPH_NUM_MDS=3
[ -z "$CEPH_NUM_RGW" ] && CEPH_NUM_RGW=1

[ -z "$CEPH_DIR" ] && CEPH_DIR="$PWD/"
[ -z "$CEPH_DEV_DIR" ] && CEPH_DEV_DIR="$CEPH_DIR/dev"
[ -z "$CEPH_OUT_DIR" ] && CEPH_OUT_DIR="$CEPH_DIR/out"
[ -z "$CEPH_RGW_PORT" ] && CEPH_RGW_PORT=8000

extra_conf=""
new=0
standby=0
debug=0
start_all=1
start_mon=0
start_mds=0
start_osd=0
start_rgw=0
ip=""
nodaemon=0
smallmds=0
hitset=""
overwrite_conf=1
cephx=1 #turn cephx on by default
cache=""
memstore=0

MON_ADDR=""

conf="$CEPH_DIR/ceph.conf"

keyring_fn="$CEPH_DIR/keyring"
osdmap_fn="/tmp/ceph_osdmap.$$"
monmap_fn="/tmp/ceph_monmap.$$"

usage="usage: $0 [option]... [mon] [mds] [osd]\n"
usage=$usage"options:\n"
usage=$usage"\t-d, --debug\n"
usage=$usage"\t-s, --standby_mds: Generate standby-replay MDS for each active\n"
usage=$usage"\t-l, --localhost: use localhost instead of hostname\n"
usage=$usage"\t-i <ip>: bind to specific ip\n"
usage=$usage"\t-r start radosgw (needs ceph compiled with --radosgw and apache2 with mod_fastcgi)\n"
usage=$usage"\t-n, --new\n"
usage=$usage"\t--valgrind[_{osd,mds,mon}] 'toolname args...'\n"
usage=$usage"\t--nodaemon: use ceph-run as wrapper for mon/osd/mds\n"
usage=$usage"\t--smallmds: limit mds cache size\n"
usage=$usage"\t-m ip:port\t\tspecify monitor address\n"
usage=$usage"\t-k keep old configuration files\n"
usage=$usage"\t-x enable cephx (on by default)\n"
usage=$usage"\t-X disable cephx\n"
usage=$usage"\t--hitset <pool> <hit_set_type>: enable hitset tracking\n"
usage=$usage"\t-o config\t\t add extra config parameters to mds section\n"

usage_exit() {
	printf "$usage"
	exit
}

while [ $# -ge 1 ]; do
case $1 in
    -d | --debug )
	    debug=1
	    ;;
    -s | --standby_mds)
	    standby=1
	    ;;
    -l | --localhost )
	    ip="127.0.0.1"
	    ;;
    -i )
	    [ -z "$2" ] && usage_exit
	    ip="$2"
	    shift
	    ;;
    -r )
	    start_rgw=1
	    ;;
    --new | -n )
	    new=1
	    ;;
    --valgrind )
	    [ -z "$2" ] && usage_exit
	    valgrind=$2
	    shift
	    ;;
    --valgrind_mds )
	    [ -z "$2" ] && usage_exit
	    valgrind_mds=$2
	    shift
	    ;;
    --valgrind_osd )
	    [ -z "$2" ] && usage_exit
	    valgrind_osd=$2
	    shift
	    ;;
    --valgrind_mon )
	    [ -z "$2" ] && usage_exit
	    valgrind_mon=$2
	    shift
	    ;;
    --nodaemon )
	    nodaemon=1
	    ;;
    --smallmds )
	    smallmds=1
	    ;;
    mon )
	    start_mon=1
	    start_all=0
	    ;;
    mds )
	    start_mds=1
	    start_all=0
	    ;;
    osd )
	    start_osd=1
	    start_all=0
	    ;;
    -m )
	    [ -z "$2" ] && usage_exit
	    MON_ADDR=$2
	    shift
	    ;;
    -x )
	    cephx=1 # this is on be default, flag exists for historical consistency
	    ;;
    -X )
	    cephx=0
	    ;;
    -k )
	    overwrite_conf=0
	    ;;
    --memstore )
	    memstore=1
	    ;;
    --hitset )
	    hitset="$hitset $2 $3"
	    shift
	    shift
	    ;;
    -o )
	    extra_conf="$extra_conf	$2
"
	    shift
	    ;;
    --cache )
	    if [ -z "$cache" ]; then
		cache="$2"
	    else
		cache="$cache $2"
	    fi
	    shift
	    ;;
    * )
	    usage_exit
esac
shift
done

if [ "$start_all" -eq 1 ]; then
	start_mon=1
	start_mds=1
	start_osd=1
fi

ARGS="-c $conf"

run() {
    type=$1
    shift
    eval "valg=\$valgrind_$type"
    [ -z "$valg" ] && valg="$valgrind"

    if [ -n "$valg" ]; then
	echo "valgrind --tool=$valg $* -f &"
	valgrind --tool=$valg $* -f &
	sleep 1
    else
	if [ "$nodaemon" -eq 0 ]; then
	    echo "$*"
	    $*
	else
	    echo "ceph-run $* -f &"
	    ./ceph-run $* -f &
	fi
    fi
}

if [ "$debug" -eq 0 ]; then
    CMONDEBUG='
	debug mon = 10
        debug ms = 1'
    COSDDEBUG='
        debug ms = 1'
    CMDSDEBUG='
        debug ms = 1'
else
    echo "** going verbose **"
    CMONDEBUG='
	debug mon = 20
        debug paxos = 20
        debug auth = 20
        debug ms = 1'
    COSDDEBUG='
        debug ms = 1
        debug osd = 25
        debug objecter = 20
        debug monc = 20
        debug journal = 20
        debug filestore = 20
        debug objclass = 20'
    CMDSDEBUG='
        debug ms = 1
        debug mds = 20
        debug auth = 20
        debug monc = 20
        mds debug scatterstat = true
        mds verify scatter = true
        mds log max segments = 2'
fi

if [ -n "$MON_ADDR" ]; then
	CMON_ARGS=" -m "$MON_ADDR
	COSD_ARGS=" -m "$MON_ADDR
	CMDS_ARGS=" -m "$MON_ADDR
fi

if [ "$memstore" -eq 1 ]; then
    COSDMEMSTORE='
	osd objectstore = memstore'
fi

# lockdep everywhere?
# export CEPH_ARGS="--lockdep 1"

[ -z "$CEPH_BIN" ] && CEPH_BIN=.
[ -z "$CEPH_PORT" ] && CEPH_PORT=6789


# sudo if btrfs
test -d $CEPH_DEV_DIR/osd0/. && test -e $CEPH_DEV_DIR/sudo && SUDO="sudo"

if [ "$start_all" -eq 1 ]; then
    $SUDO $CEPH_BIN/init-ceph stop
fi
$SUDO rm -f core*

test -d $CEPH_OUT_DIR || mkdir $CEPH_OUT_DIR
test -d $CEPH_DEV_DIR || mkdir $CEPH_DEV_DIR
$SUDO rm -rf $CEPH_OUT_DIR/*
test -d gmon && $SUDO rm -rf gmon/*

[ "$cephx" -eq 1 ] && [ "$new" -eq 1 ] && test -e $keyring_fn && rm $keyring_fn


# figure machine's ip
HOSTNAME=`hostname`
if [ -n "$ip" ]; then
    IP="$ip"
else
    echo hostname $HOSTNAME
    RAW_IP=`hostname -I`
    # filter out IPv6 and localhost addresses
    IP="$(echo "$RAW_IP"|tr ' ' '\012'|grep -v :|grep -v '^127\.'|head -n1)"
    # if that left nothing, then try to use the raw thing, it might work
    if [ -z "$IP" ]; then IP="$RAW_IP"; fi
    echo ip $IP
fi
echo "ip $IP"



if [ "$cephx" -eq 1 ]; then
    CEPH_ADM="$CEPH_BIN/ceph -c $conf -k $keyring_fn"
else
    CEPH_ADM="$CEPH_BIN/ceph -c $conf"
fi

MONS=""
count=0
for f in a b c d e f g h i j k l m n o p q r s t u v w x y z
do
    if [ -z "$MONS" ];
    then
	MONS="$f"
    else
	MONS="$MONS $f"
    fi
    count=$(($count + 1))
    [ $count -eq $CEPH_NUM_MON ] && break;
done

DAEMONOPTS="
	log file = $CEPH_OUT_DIR/\$name.log
        admin socket = $CEPH_OUT_DIR/\$name.asok
	chdir = \"\"
	pid file = $CEPH_OUT_DIR/\$name.pid
        heartbeat file = $CEPH_OUT_DIR/\$name.heartbeat
"


if [ "$start_mon" -eq 1 ]; then

	if [ "$new" -eq 1 ]; then
		if [ $overwrite_conf -eq 1 ]; then
		        cat <<EOF > $conf
; generated by vstart.sh on `date`
[global]
        fsid = $(uuidgen)
        osd pg bits = 3
        osd pgp bits = 5  ; (invalid, but ceph should cope!)
        osd crush chooseleaf type = 0
        osd pool default min size = 1
        osd pool default erasure code directory = .libs
        osd pool default erasure code profile = plugin=jerasure technique=reed_sol_van k=2 m=1 ruleset-failure-domain=osd
        run dir = $CEPH_OUT_DIR
EOF
if [ "$cephx" -eq 1 ] ; then
cat <<EOF >> $conf
        auth supported = cephx
EOF
else
cat <<EOF >> $conf
	auth cluster required = none
	auth service required = none
	auth client required = none
EOF
fi
			cat <<EOF >> $conf

[client]
        keyring = $keyring_fn
        log file = $CEPH_OUT_DIR/\$name.\$pid.log

[mds]
$DAEMONOPTS
$CMDSDEBUG
        mds debug frag = true
        mds debug auth pins = true
        mds debug subtrees = true
        mds data = $CEPH_DEV_DIR/mds.\$id
$extra_conf
[osd]
$DAEMONOPTS
        osd data = $CEPH_DEV_DIR/osd\$id
        osd journal = $CEPH_DEV_DIR/osd\$id.journal
        osd journal size = 100
        osd class tmp = out
        osd class dir = .libs
        osd scrub load threshold = 5.0
        osd debug op order = true
$COSDDEBUG
$COSDMEMSTORE
$extra_conf
[mon]
        mon pg warn min per osd = 10
        mon osd allow primary affinity = true
$DAEMONOPTS
$CMONDEBUG
$extra_conf
        mon cluster log file = $CEPH_OUT_DIR/cluster.mon.\$id.log
[global]
$extra_conf
EOF
		fi

		if [ `echo $IP | grep '^127\\.'` ]
		then
			echo
			echo "NOTE: hostname resolves to loopback; remote hosts will not be able to"
			echo "  connect.  either adjust /etc/hosts, or edit this script to use your"
			echo "  machine's real IP."
			echo
		fi

	        $SUDO $CEPH_BIN/ceph-authtool --create-keyring --gen-key --name=mon. $keyring_fn --cap mon 'allow *'
	        $SUDO $CEPH_BIN/ceph-authtool --gen-key --name=client.admin --set-uid=0 \
		    --cap mon 'allow *' \
		    --cap osd 'allow *' \
		    --cap mds allow \
		    $keyring_fn

		# build a fresh fs monmap, mon fs
		str="$CEPH_BIN/monmaptool --create --clobber"
		count=0
		for f in $MONS
		do
			str=$str" --add $f $IP:$(($CEPH_PORT+$count))"
			if [ $overwrite_conf -eq 1 ]; then
				cat <<EOF >> $conf
[mon.$f]
        host = $HOSTNAME
        mon data = $CEPH_DEV_DIR/mon.$f
        mon addr = $IP:$(($CEPH_PORT+$count))
EOF
			fi
			count=$(($count + 1))
		done
		str=$str" --print $monmap_fn"
		echo $str
		$str

		for f in $MONS
		do
		    cmd="rm -rf $CEPH_DEV_DIR/mon.$f"
		    echo $cmd
		    $cmd
                    cmd="mkdir -p $CEPH_DEV_DIR/mon.$f"
                    echo $cmd
                    $cmd
		    cmd="$CEPH_BIN/ceph-mon --mkfs -c $conf -i $f --monmap=$monmap_fn"
		    cmd="$cmd --keyring=$keyring_fn"
		    echo $cmd
		    $cmd
		done

		rm $monmap_fn
	fi

	# start monitors
	if [ "$start_mon" -ne 0 ]; then
		for f in $MONS
		do
		    run 'mon' $CEPH_BIN/ceph-mon -i $f $ARGS $CMON_ARGS
		done
	fi
fi

#osd
if [ "$start_osd" -eq 1 ]; then
    for osd in `seq 0 $((CEPH_NUM_OSD-1))`
    do
	if [ "$new" -eq 1 ]; then
	    if [ $overwrite_conf -eq 1 ]; then
		    cat <<EOF >> $conf
[osd.$osd]
        host = $HOSTNAME
EOF
		    rm -rf $CEPH_DEV_DIR/osd$osd || true
		    for f in $CEPH_DEV_DIR/osd$osd/* ; do btrfs sub delete $f || true ; done || true
		    mkdir -p $CEPH_DEV_DIR/osd$osd
	    fi

	    uuid=`uuidgen`
	    echo "add osd$osd $uuid"
	    $SUDO $CEPH_ADM osd create $uuid
	    $SUDO $CEPH_ADM osd crush add osd.$osd 1.0 host=$HOSTNAME root=default
	    $SUDO $CEPH_BIN/ceph-osd -i $osd $ARGS --mkfs --mkkey --osd-uuid $uuid

	    key_fn=$CEPH_DEV_DIR/osd$osd/keyring
	    echo adding osd$osd key to auth repository
	    $SUDO $CEPH_ADM -i $key_fn auth add osd.$osd osd "allow *" mon "allow profile osd"
	fi
	echo start osd$osd
	run 'osd' $SUDO $CEPH_BIN/ceph-osd -i $osd $ARGS $COSD_ARGS
    done
fi

# mds
if [ "$smallmds" -eq 1 ]; then
    cat <<EOF >> $conf
[mds]
	mds log max segments = 2
	mds cache size = 10000
EOF
fi

if [ "$start_mds" -eq 1 -a "$CEPH_NUM_MDS" -gt 0 ]; then
    mds=0
    for name in a b c d e f g h i j k l m n o p
    do
	if [ "$new" -eq 1 ]; then
	    mkdir -p $CEPH_DEV_DIR/mds.$name
	    key_fn=$CEPH_DEV_DIR/mds.$name/keyring
	    if [ $overwrite_conf -eq 1 ]; then
	    	cat <<EOF >> $conf
[mds.$name]
        host = $HOSTNAME
EOF
		if [ "$standby" -eq 1 ]; then
		    mkdir -p $CEPH_DEV_DIR/mds.${name}s
		    cat <<EOF >> $conf
       mds standby for rank = $mds
[mds.${name}s]
        mds standby replay = true
        mds standby for name = ${name}
EOF
		fi
	    fi
	    $SUDO $CEPH_BIN/ceph-authtool --create-keyring --gen-key --name=mds.$name $key_fn
	    $SUDO $CEPH_ADM -i $key_fn auth add mds.$name mon 'allow profile mds' osd 'allow *' mds 'allow'
	    if [ "$standby" -eq 1 ]; then
		    $SUDO $CEPH_BIN/ceph-authtool --create-keyring --gen-key --name=mds.${name}s \
			$CEPH_DEV_DIR/mds.${name}s/keyring
                    $SUDO $CEPH_ADM -i $CEPH_DEV_DIR/mds.${name}s/keyring auth add mds.${name}s \
			mon 'allow *' osd 'allow *' mds 'allow'
	    fi
	fi

	run 'mds' $CEPH_BIN/ceph-mds -i $name $ARGS $CMDS_ARGS
	if [ "$standby" -eq 1 ]; then
	    run 'mds' $CEPH_BIN/ceph-mds -i ${name}s $ARGS $CMDS_ARGS
	fi

	mds=$(($mds + 1))
	[ $mds -eq $CEPH_NUM_MDS ] && break

#valgrind --tool=massif $CEPH_BIN/ceph-mds $ARGS --mds_log_max_segments 2 --mds_thrash_fragments 0 --mds_thrash_exports 0 > m  #--debug_ms 20
#$CEPH_BIN/ceph-mds -d $ARGS --mds_thrash_fragments 0 --mds_thrash_exports 0 #--debug_ms 20
#$CEPH_ADM mds set max_mds 2
    done
    cmd="$CEPH_ADM mds set max_mds $CEPH_NUM_MDS"
    echo $cmd
    $cmd
fi


# rgw
if [ "$start_rgw" -eq 1 ]; then
    for rgw in `seq 0 $((CEPH_NUM_RGW-1))`
    do
	rgwport=$(( $CEPH_RGW_PORT + $rgw ))
	if [ "$new" -eq 1 ]; then
	    if [ $overwrite_conf -eq 1 ]; then
		    dnsname=`hostname -f`
		    cat <<EOF >> $conf
[client.radosgw.rgw$rgw]
        host = $HOSTNAME
$DAEMONOPTS
        keyring = $CEPH_OUT_DIR/keyring.client.radosgw.rgw$rgw
        rgw socket path = $CEPH_OUT_DIR/sock.client.radosgw.rgw$rgw
        rgw dns name = $dnsname
EOF
		    mkdir -p $CEPH_OUT_DIR/htdocs
		    mkdir -p $CEPH_OUT_DIR/fastcgi_sock
		    APACHE2_MODULE_PATH="/usr/lib/apache2/modules"
		    APACHE2_EXTRA_MODULES_NAME="mpm_prefork authz_core"
		    for module in $APACHE2_EXTRA_MODULES_NAME
		    do
			    if [ -f "${APACHE2_MODULE_PATH}/mod_${module}.so" ]; then
				    APACHE2_EXTRA_MODULES="${APACHE2_EXTRA_MODULES}LoadModule ${module}_module ${APACHE2_MODULE_PATH}/mod_${module}.so
"
			    fi
		    done
		    echo $APACHE2_EXTRA_MODULES
		    cat <<EOF > $CEPH_OUT_DIR/apache.conf
LoadModule env_module /usr/lib/apache2/modules/mod_env.so
LoadModule rewrite_module /usr/lib/apache2/modules/mod_rewrite.so
LoadModule fastcgi_module /usr/lib/apache2/modules/mod_fastcgi.so
$APACHE2_EXTRA_MODULES

Listen $rgwport
ServerName rgwtest.example.com

ServerRoot $CEPH_OUT_DIR
ErrorLog $CEPH_OUT_DIR/apache.error.log
LogFormat "%h l %u %t \"%r\" %>s %b \"{Referer}i\" \"%{User-agent}i\"" combined
CustomLog $CEPH_OUT_DIR/apache.access.log combined
PidFile $CEPH_OUT_DIR/apache.pid
DocumentRoot $CEPH_OUT_DIR/htdocs
FastCgiIPCDir $CEPH_OUT_DIR/fastcgi_sock
FastCgiExternalServer $CEPH_OUT_DIR/htdocs/rgw.fcgi -socket $CEPH_OUT_DIR/sock.client.radosgw.rgw$rgw
RewriteEngine On

RewriteRule ^/([a-zA-Z0-9-_.]*)([/]?.*) /rgw.fcgi?page=$1&params=$2&%{QUERY_STRING} [E=HTTP_AUTHORIZATION:%{HTTP:Authorization},L]

# Set fastcgi environment variables.
# Note that this is separate from Unix environment variables!
SetEnv RGW_LOG_LEVEL 20
SetEnv RGW_PRINT_CONTINUE yes
SetEnv RGW_SHOULD_LOG yes

<Directory $CEPH_OUT_DIR/htdocs>
  Options +ExecCGI
  AllowOverride All
  SetHandler fastcgi-script
</Directory>

AllowEncodedSlashes On
ServerSignature Off
EOF
		    $SUDO $CEPH_ADM auth get-or-create client.radosgw.rgw$rgw osd 'allow rwx' mon 'allow r' -o $CEPH_OUT_DIR/keyring.client.radosgw.rgw$rgw

		    #akey=`echo $$ | md5sum | cut -c 1-20`
		    #skey=`dd if=/dev/urandom of=/tmp/random.$$ bs=1 count=40 2>/dev/null ; base64 < /tmp/random.$$ ; rm /tmp/random.$$`
		    akey='0555b35654ad1656d804'
		    skey='h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q=='
		    echo access key $akey
		    echo secret key $skey
		    $CEPH_BIN/radosgw-admin user create --uid tester --access-key $akey --secret $skey --display-name 'M. Tester' --email tester@ceph.com -c $conf
	    fi
	fi
	echo start rgw$rgw on http://localhost:$rgwport
	run 'rgw' $SUDO $CEPH_BIN/radosgw -n client.radosgw.rgw$rgw $ARGS
	run 'apache2' $SUDO apache2 -f $CEPH_OUT_DIR/apache.conf
    done
fi

echo "started.  stop.sh to stop.  see out/* (e.g. 'tail -f out/????') for debug output."

do_cache() {
    while [ -n "$*" ]; do
	p="$1"
	shift
	echo "creating cache for pool $p ..."
	$SUDO $CEPH_ADM <<EOF
osd pool create ${p}-cache 8
osd tier add $p ${p}-cache
osd tier cache-mode ${p}-cache writeback
osd tier set-overlay $p ${p}-cache
quit
EOF
    done
}
do_cache $cache

do_hitsets() {
    while [ -n "$*" ]; do
	pool="$1"
	type="$2"
	shift
	shift
	echo "setting hit_set on pool $pool type $type ..."
	$CEPH_ADM <<EOF
osd pool set $pool hit_set_type $type
osd pool set $pool hit_set_count 8
osd pool set $pool hit_set_period 30
quit
EOF
    done
}
do_hitsets $hitset

echo ""
echo "export PYTHONPATH=./pybind"
echo "export LD_LIBRARY_PATH=.libs"

