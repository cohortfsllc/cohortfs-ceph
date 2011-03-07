#!/bin/bash

set -e

vers=$1
debsubver=$2

[ -z "$debsubver" ] && debsubver="1"

[ -z "$vers" ] && [ -e .last_release ] && vers=`cat .last_release`
[ -z "$vers" ] && echo specify version && exit 1
echo version $vers

#./pull.sh $vers dsc changes

for f in `cd release/$vers ; ls *-$debsubver*.{dsc,changes}`
do
    if [ -e "release/$vers/$f" ]; then
	if head -1 release/$vers/$f | grep -q 'BEGIN PGP SIGNED MESSAGE' ; then
	    echo already signed $f
	else
	    debsign -k288995c8 release/$vers/$f
	fi
    fi
done
