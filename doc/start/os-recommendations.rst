====================
 OS Recommendations
====================

Ceph Dependencies
=================

As a general rule, we recommend deploying Ceph on newer releases of Linux.

Linux Kernel
------------

- **Ceph Kernel Client:**  We currently recommend:

  - v3.6.6 or later in the v3.6 stable series
  - v3.4.20 or later in the v3.4 stable series

- **btrfs**: If you use the ``btrfs`` file system with Ceph, we recommend using
  a recent Linux kernel (v3.5 or later).

glibc
-----

- **syncfs(2)**: For non-btrfs filesystems such as XFS and ext4 where
  more than one ``ceph-osd`` daemon is used on a single server, Ceph
  performs signficantly better with the ``syncfs(2)`` system call
  (added in kernel 2.6.39 and glibc 2.14).  New versions of Ceph (v0.55 and
  later) do not depend on glibc support.


Platforms
=========

The charts below show how Ceph's requirements map onto various Linux
platforms.  Generally speaking, there is very little dependence on
specific distributions aside from the kernel and system initialization
package (i.e., sysvinit, upstart, systemd).


Emperor (0.72)
--------------

+----------+----------+--------------------+--------------+---------+------------+
| Distro   | Release  | Code Name          | Kernel       | Notes   | Testing    |
+==========+==========+====================+==============+=========+============+
| Ubuntu   | 12.04    | Precise Pangolin   | linux-3.2.0  | 1, 2    | B, I, C    |
+----------+----------+--------------------+--------------+---------+------------+
| Ubuntu   | 12.10    | Quantal Quetzal    | linux-3.5.4  | 2, 4    | B          |
+----------+----------+--------------------+--------------+---------+------------+
| Ubuntu   | 13.04    | Raring Ringtail    | linux-3.8.5  | 4       | B          |
+----------+----------+--------------------+--------------+---------+------------+
| Debian   | 6.0      | Squeeze            | linux-2.6.32 | 1, 2, 3 | B          |
+----------+----------+--------------------+--------------+---------+------------+
| Debian   | 7.0      | Wheezy             | linux-3.2.0  | 1, 2    | B          |
+----------+----------+--------------------+--------------+---------+------------+
| CentOS   | 6.3      | N/A                | linux-2.6.32 | 1, 2    | B, I       |
+----------+----------+--------------------+--------------+---------+------------+
| RHEL     | 6.3      |                    | linux-2.6.32 | 1, 2    | B, I       |
+----------+----------+--------------------+--------------+---------+------------+
| Fedora   | 18.0     | Spherical Cow      | linux-3.6.0  |         | B          |
+----------+----------+--------------------+--------------+---------+------------+
| Fedora   | 19.0     | Schrödinger's Cat  | linux-3.10.0 |         | B          |
+----------+----------+--------------------+--------------+---------+------------+
| OpenSuse | 12.2     | N/A                | linux-3.4.0  | 2       | B          |
+----------+----------+--------------------+--------------+---------+------------+



Dumpling (0.67)
---------------

+----------+----------+--------------------+--------------+---------+------------+
| Distro   | Release  | Code Name          | Kernel       | Notes   | Testing    |
+==========+==========+====================+==============+=========+============+
| Ubuntu   | 12.04    | Precise Pangolin   | linux-3.2.0  | 1, 2    | B, I, C    |
+----------+----------+--------------------+--------------+---------+------------+
| Ubuntu   | 12.10    | Quantal Quetzal    | linux-3.5.4  | 2       | B          |
+----------+----------+--------------------+--------------+---------+------------+
| Ubuntu   | 13.04    | Raring Ringtail    | linux-3.8.5  |         | B          |
+----------+----------+--------------------+--------------+---------+------------+
| Debian   | 6.0      | Squeeze            | linux-2.6.32 | 1, 2, 3 | B          |
+----------+----------+--------------------+--------------+---------+------------+
| Debian   | 7.0      | Wheezy             | linux-3.2.0  | 1, 2    | B          |
+----------+----------+--------------------+--------------+---------+------------+
| CentOS   | 6.3      | N/A                | linux-2.6.32 | 1, 2    | B, I       |
+----------+----------+--------------------+--------------+---------+------------+
| RHEL     | 6.3      |                    | linux-2.6.32 | 1, 2    | B, I       |
+----------+----------+--------------------+--------------+---------+------------+
| Fedora   | 18.0     | Spherical Cow      | linux-3.6.0  |         | B          |
+----------+----------+--------------------+--------------+---------+------------+
| Fedora   | 19.0     | Schrödinger's Cat  | linux-3.10.0 |         | B          |
+----------+----------+--------------------+--------------+---------+------------+
| OpenSuse | 12.2     | N/A                | linux-3.4.0  | 2       | B          |
+----------+----------+--------------------+--------------+---------+------------+



Cuttlefish (0.61)
-----------------

+----------+----------+--------------------+--------------+---------+------------+
| Distro   | Release  | Code Name          | Kernel       | Notes   | Testing    |
+==========+==========+====================+==============+=========+============+
| Ubuntu   | 12.04    | Precise Pangolin   | linux-3.2.0  | 1, 2    | B, I, C    |
+----------+----------+--------------------+--------------+---------+------------+
| Ubuntu   | 12.10    | Quantal Quetzal    | linux-3.5.4  | 2       | B          |
+----------+----------+--------------------+--------------+---------+------------+
| Ubuntu   | 13.04    | Raring Ringtail    | linux-3.8.5  |         | B          |
+----------+----------+--------------------+--------------+---------+------------+
| Debian   | 6.0      | Squeeze            | linux-2.6.32 | 1, 2, 3 | B          |
+----------+----------+--------------------+--------------+---------+------------+
| Debian   | 7.0      | Wheezy             | linux-3.2.0  | 1, 2    | B          |
+----------+----------+--------------------+--------------+---------+------------+
| CentOS   | 6.3      | N/A                | linux-2.6.32 | 1, 2    | B, I       |
+----------+----------+--------------------+--------------+---------+------------+
| RHEL     | 6.3      |                    | linux-2.6.32 | 1, 2    | B, I       |
+----------+----------+--------------------+--------------+---------+------------+
| Fedora   | 17.0     | Beefy Miracle      | linux-3.3.4  | 1, 2    | B          |
+----------+----------+--------------------+--------------+---------+------------+
| Fedora   | 18.0     | Spherical Cow      | linux-3.6.0  |         | B          |
+----------+----------+--------------------+--------------+---------+------------+
| OpenSuse | 12.2     | N/A                | linux-3.4.0  | 2       | B          |
+----------+----------+--------------------+--------------+---------+------------+


Bobtail (0.56)
--------------

+----------+----------+--------------------+--------------+---------+------------+
| Distro   | Release  | Code Name          | Kernel       | Notes   | Testing    |
+==========+==========+====================+==============+=========+============+
| Ubuntu   | 11.04    | Natty Narwhal      | linux-2.6.38 | 1, 2, 3 | B          |
+----------+----------+--------------------+--------------+---------+------------+
| Ubuntu   | 11.10    | Oneric Ocelot      | linux-3.0.0  | 1, 2    | B          |
+----------+----------+--------------------+--------------+---------+------------+
| Ubuntu   | 12.04    | Precise Pangolin   | linux-3.2.0  | 1, 2    | B, I, C    |
+----------+----------+--------------------+--------------+---------+------------+
| Ubuntu   | 12.10    | Quantal Quetzal    | linux-3.5.4  | 2       | B          |
+----------+----------+--------------------+--------------+---------+------------+
| Debian   | 6.0      | Squeeze            | linux-2.6.32 | 1, 2, 3 | B          |
+----------+----------+--------------------+--------------+---------+------------+
| Debian   | 7.0      | Wheezy             | linux-3.2.0  | 1, 2    | B          |
+----------+----------+--------------------+--------------+---------+------------+
| CentOS   | 6.3      | N/A                | linux-2.6.32 | 1, 2    | B, I       |
+----------+----------+--------------------+--------------+---------+------------+
| Fedora   | 17.0     | Beefy Miracle      | linux-3.3.4  | 1, 2    | B          |
+----------+----------+--------------------+--------------+---------+------------+
| Fedora   | 18.0     | Spherical Cow      | linux-3.6.0  |         | B          |
+----------+----------+--------------------+--------------+---------+------------+
| OpenSuse | 12.2     | N/A                | linux-3.4.0  | 2       | B          |
+----------+----------+--------------------+--------------+---------+------------+


Argonaut (0.48)
---------------

+----------+----------+--------------------+--------------+---------+------------+
| Distro   | Release  | Code Name          | Kernel       | Notes   | Testing    |
+==========+==========+====================+==============+=========+============+
| Ubuntu   | 11.04    | Natty Narwhal      | linux-2.6.38 | 1, 2, 3 | B          |
+----------+----------+--------------------+--------------+---------+------------+
| Ubuntu   | 11.10    | Oneric Ocelot      | linux-3.0.0  | 1, 2, 3 | B          |
+----------+----------+--------------------+--------------+---------+------------+
| Ubuntu   | 12.04    | Precise Pangolin   | linux-3.2.0  | 1, 2    | B, I, C    |
+----------+----------+--------------------+--------------+---------+------------+
| Ubuntu   | 12.10    | Quantal Quetzal    | linux-3.5.4  | 2       | B          |
+----------+----------+--------------------+--------------+---------+------------+
| Debian   | 6.0      | Squeeze            | linux-2.6.32 | 1, 2, 3 | B          |
+----------+----------+--------------------+--------------+---------+------------+
| Debian   | 7.0      | Wheezy             | linux-3.2.0  | 1, 2, 3 | B          |
+----------+----------+--------------------+--------------+---------+------------+


Notes
-----

- **1**: The default kernel has an older version of ``btrfs`` that we do not
  recommend for ``ceph-osd`` storage nodes.  Upgrade to a recommended
  kernel or use ``XFS`` or ``ext4``.

- **2**: The default kernel has an old Ceph client that we do not recommend
  for kernel client (kernel RBD or the Ceph file system).  Upgrade to a
  recommended kernel.

- **3**: The default kernel or installed version of ``glibc`` does not
  support the ``syncfs(2)`` system call.  Putting multiple
  ``ceph-osd`` daemons using ``XFS`` or ``ext4`` on the same host will
  not perform as well as they could.

- **4**: Ceph provides ARM support for Quantal and Raring. Saucy support is
  not supported yet, but support is coming soon.

Testing
-------

- **B**: We continuously build all branches on this platform and exercise basic
  unit tests.  We build release packages for this platform.

- **I**: We do basic installation and functionality tests of releases on this
  platform.

- **C**: We run a comprehensive functional, regression, and stress test suite
  on this platform on a continuous basis. This includes development branches,
  pre-release, and released code.

