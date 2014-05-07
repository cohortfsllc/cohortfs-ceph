===============================================
 rbd -- manage rados block device (RBD) images
===============================================

.. program:: rbd

Synopsis
========

| **rbd** [ -c *ceph.conf* ] [ -m *monaddr* ] [ -p | --pool *pool* ] [
  --size *size* ] [ --order *bits* ] [ *command* ... ]


Description
===========

**rbd** is a utility for manipulating rados block device (RBD) images,
used by the Linux rbd driver and the rbd storage driver for Qemu/KVM.
RBD images are simple block devices that are striped over objects and
stored in a RADOS object store. The size of the objects the image is
striped over must be a power of two.


Options
=======

.. option:: -c ceph.conf, --conf ceph.conf

   Use ceph.conf configuration file instead of the default /etc/ceph/ceph.conf to
   determine monitor addresses during startup.

.. option:: -m monaddress[:port]

   Connect to specified monitor (instead of looking through ceph.conf).

.. option:: -p pool, --pool pool

   Interact with the given pool. Required by most commands.

.. option:: --no-progress

   Do not output progress information (goes to standard error by
   default for some commands).


Parameters
==========

.. option:: --image-format format

   Specifies which object layout to use. The default is 1.

   * format 1 - Use the original format for a new rbd image. This format is
     understood by all versions of librbd and the kernel rbd module, but
     does not support newer features.

   * format 2 - Use the second rbd format, which is supported by
     librbd (but not the kernel rbd module) at this time. This is more
     easily extensible to allow more features in the future.

.. option:: --size size-in-mb

   Specifies the size (in megabytes) of the new rbd image.

.. option:: --order bits

   Specifies the object size expressed as a number of bits, such that
   the object size is ``1 << order``. The default is 22 (4 MB).

.. option:: --stripe-unit size-in-bytes

   Specifies the stripe unit size in bytes.  See striping section (below) for more details.

.. option:: --stripe-count num

   Specifies the number of objects to stripe over before looping back
   to the first object.  See striping section (below) for more details.

.. option:: --id username

   Specifies the username (without the ``client.`` prefix) to use with the map command.

.. option:: --keyfile filename

   Specifies a file containing the secret to use with the map command.
   If not specified, ``client.admin`` will be used by default.

.. option:: --keyring filename

   Specifies a keyring file containing a secret for the specified user
   to use with the map command.  If not specified, the default keyring
   locations will be searched.

.. option:: --shared tag

   Option for `lock add` that allows multiple clients to lock the
   same image if they use the same tag. The tag is an arbitrary
   string. This is useful for situations where an image must
   be open from more than one client at once, like during
   live migration of a virtual machine, or for use underneath
   a clustered filesystem.

.. option:: --format format

   Specifies output formatting (default: plain, json, xml)

.. option:: --pretty-format

   Make json or xml formatted output more human-readable.

.. option:: -o map-options, --options map-options

   Specifies which options to use when mapping an image.  map-options is
   a comma-separated string of options (similar to mount(8) mount options).
   See map options section below for more details.

.. option:: --read-only

   Map the image read-only.  Equivalent to -o ro.


Commands
========

.. TODO rst "option" directive seems to require --foo style options, parsing breaks on subcommands.. the args show up as bold too

:command:`ls` [-l | --long] [pool-name]
  Will list all rbd images listed in the rbd_directory object.  With
  -l, use longer-format output including size, format, etc.

:command:`info` [*image-name*]
  Will dump information (such as size and order) about a specific rbd image.

:command:`create` [*image-name*]
  Will create a new rbd image. You must also specify the size via --size.  The
  --stripe-unit and --stripe-count arguments are optional, but must be used together.

:command:`resize` [*image-name*] [--allow-shrink]
  Resizes rbd image. The size parameter also needs to be specified.
  The --allow-shrink option lets the size be reduced.

:command:`rm` [*image-name*]
  Deletes an rbd image (including all data blocks).

:command:`export` [*image-name*] [*dest-path*]
  Exports image to dest path (use - for stdout).

:command:`import` [*path*] [*dest-image*]
  Creates a new image and imports its data from path (use - for
  stdin).  The import operation will try to create sparse rbd images 
  if possible.  For import from stdin, the sparsification unit is
  the data block size of the destination image (1 << order).

:command:`cp` [*src-image*] [*dest-image*]
  Copies the content of a src-image into the newly created dest-image.
  dest-image will have the same size, order, and image format as src-image.

:command:`mv` [*src-image*] [*dest-image*]
  Renames an image.  Note: rename across pools is not supported.

:command:`map` [*image-name*] [-o | --options *map-options* ] [--read-only]
  Maps the specified image to a block device via the rbd kernel module.

:command:`unmap` [*device-path*]
  Unmaps the block device that was mapped via the rbd kernel module.

:command:`showmapped`
  Show the rbd images that are mapped via the rbd kernel module.

:command:`lock` list [*image-name*]
  Show locks held on the image. The first column is the locker
  to use with the `lock remove` command.

:command:`lock` add [*image-name*] [*lock-id*]
  Lock an image. The lock-id is an arbitrary name for the user's
  convenience. By default, this is an exclusive lock, meaning it
  will fail if the image is already locked. The --shared option
  changes this behavior. Note that locking does not affect
  any operation other than adding a lock. It does not
  protect an image from being deleted.

:command:`lock` remove [*image-name*] [*lock-id*] [*locker*]
  Release a lock on an image. The lock id and locker are
  as output by lock ls.

:command:`bench-write` [*image-name*] --io-size [*io-size-in-bytes*] --io-threads [*num-ios-in-flight*] --io-total [*total-bytes-to-write*]
  Generate a series of sequential writes to the image and measure the
  write throughput and latency.  Defaults are: --io-size 4096, --io-threads 16, 
  --io-total 1GB

Image name
==========

In addition to using the --pool option, the image name can include the
pool name. The image name format is as follows::

       [pool/]image-name

Thus an image name that contains a slash character ('/') requires specifying the pool
name explicitly.


Striping
========

RBD images are striped over many objects, which are then stored by the
Ceph distributed object store (RADOS).  As a result, read and write
requests for the image are distributed across many nodes in the
cluster, generally preventing any single node from becoming a
bottleneck when individual images get large or busy.

The striping is controlled by three parameters:

.. option:: order
  The size of objects we stripe over is a power of two, specifially 2^[*order*] bytes.  The default
  is 22, or 4 MB.

.. option:: stripe_unit
  Each [*stripe_unit*] contiguous bytes are stored adjacently in the same object, before we move on
  to the next object.

.. option:: stripe_count
  After we write [*stripe_unit*] bytes to [*stripe_count*] objects, we loop back to the initial object
  and write another stripe, until the object reaches its maximum size (as specified by [*order*].  At that
  point, we move on to the next [*stripe_count*] objects.

By default, [*stripe_unit*] is the same as the object size and [*stripe_count*] is 1.  Specifying a different
[*stripe_unit*] requires that the STRIPINGV2 feature be supported (added in Ceph v0.53) and format 2 images be
used.


Map options
===========

Most of these options are useful mainly for debugging and benchmarking.  The
default values are set in the kernel and may therefore depend on the version of
the running kernel.

* fsid=aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee - FSID that should be assumed by
  the client.

* ip=a.b.c.d[:p] - IP and, optionally, port the client should use.

* share - Enable sharing of client instances with other mappings (default).

* noshare - Disable sharing of client instances with other mappings.

* crc - Enable CRC32C checksumming for data writes (default).

* nocrc - Disable CRC32C checksumming for data writes.

* osdkeepalive=x - OSD keepalive timeout (default is 5 seconds).

* osd_idle_ttl=x - OSD idle TTL (default is 60 seconds).

* rw - Map the image read-write (default).

* ro - Map the image read-only.  Equivalent to --read-only.


Examples
========

To create a new rbd image that is 100 GB::

       rbd -p mypool create myimage --size 102400

or alternatively::

       rbd create mypool/myimage --size 102400

To use a non-default object size (8 MB)::

       rbd create mypool/myimage --size 102400 --order 23

To delete an rbd image (be careful!)::

       rbd rm mypool/myimage

To map an image via the kernel with cephx enabled::

       rbd map mypool/myimage --id admin --keyfile secretfile

To unmap an image::

       rbd unmap /dev/rbd0

To create an image with a smaller stripe_unit (to better distribute small writes in some workloads)::

       rbd -p mypool create myimage --size 102400 --stripe-unit 65536 --stripe-count 16

To change an image from one image format to another, export it and then
import it as the desired image format::

       rbd export mypool/myimage /tmp/img
       rbd import --image-format 2 /tmp/img mypool/myimage2

To lock an image for exclusive use::

       rbd lock add mypool/myimage mylockid

To release a lock::

       rbd lock remove mypool/myimage mylockid client.2485


Availability
============

**rbd** is part of the Ceph distributed storage system. Please refer to
the Ceph documentation at http://ceph.com/docs for more information.


See also
========

:doc:`ceph <ceph>`\(8),
:doc:`rados <rados>`\(8)
