
mkdir -p /ceph-backing
mount tmpfs -t tmpfs -o defaults,noatime,size=2000M,mode=777 /ceph-backing
dd if=/dev/zero of=/ceph-backing/datafile bs=1M count=2000
mkfs -t xfs /ceph-backing/datafile
mount -o loop /ceph-backing/datafile /ceph-data/
dd if=/dev/zero of=/ceph-data/bf bs=1M count=200