libclient_la_SOURCES = \
	client/Capability.cc \
	client/Client.cc \
	client/Inode.cc \
	client/InodeCache.cc \
	client/Dentry.cc \
	client/DirStripe.cc \
	client/MetaRequest.cc \
	client/ClientSnapRealm.cc \
	client/MetaSession.cc \
	client/Trace.cc
libclient_la_LIBADD = $(LIBOSDC) $(LIBEDIT_LIBS)
noinst_LTLIBRARIES += libclient.la

noinst_HEADERS += \
	client/Capability.h \
	client/Client.h \
	client/Dentry.h \
	client/DirStripe.h \
	client/Fh.h \
	client/Inode.h \
	client/InodeCache.h \
	client/MetaRequest.h \
	client/MetaSession.h \
	client/ClientSnapRealm.h \
	client/SyntheticClient.h \
	client/Trace.h \
	client/ioctl.h \
	client/ObjecterWriteback.h

if WITH_FUSE
libclient_fuse_la_SOURCES = client/fuse_ll.cc
libclient_fuse_la_LIBADD = libclient.la -lfuse
noinst_LTLIBRARIES += libclient_fuse.la
noinst_HEADERS += client/fuse_ll.h
endif

ceph_test_ioctls_SOURCES = client/test_ioctls.c
bin_DEBUGPROGRAMS += ceph_test_ioctls

