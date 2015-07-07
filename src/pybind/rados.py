"""
This module is a thin wrapper around librados.

Copyright 2011, Hannu Valtonen <hannu.valtonen@ormod.com>
"""
from ctypes import CDLL, c_char_p, c_size_t, c_void_p, c_char, c_int, c_long, \
    c_ulong, create_string_buffer, byref, Structure, c_uint64, c_ubyte, \
    pointer, CFUNCTYPE
from ctypes.util import find_library
import ctypes
import errno
import threading
import time
from datetime import datetime

ANONYMOUS_AUID = 0xffffffffffffffff
ADMIN_AUID = 0

class Error(Exception):
    """ `Error` class, derived from `Exception` """
    pass

class InterruptedOrTimeoutError(Error):
    """ `InterruptedOrTimeoutError` class, derived from `Error` """
    pass

class PermissionError(Error):
    """ `PermissionError` class, derived from `Error` """
    pass

class ObjectNotFound(Error):
    """ `ObjectNotFound` class, derived from `Error` """
    pass

class NoData(Error):
    """ `NoData` class, derived from `Error` """
    pass

class ObjectExists(Error):
    """ `ObjectExists` class, derived from `Error` """
    pass

class IOError(Error):
    """ `IOError` class, derived from `Error` """
    pass

class NoSpace(Error):
    """ `NoSpace` class, derived from `Error` """
    pass

class IncompleteWriteError(Error):
    """ `IncompleteWriteError` class, derived from `Error` """
    pass

class RadosStateError(Error):
    """ `RadosStateError` class, derived from `Error` """
    pass

class IoctxStateError(Error):
    """ `IoctxStateError` class, derived from `Error` """
    pass

class ObjectStateError(Error):
    """ `ObjectStateError` class, derived from `Error` """
    pass

class LogicError(Error):
    """ `` class, derived from `Error` """
    pass

class TimedOut(Error):
    """ `TimedOut` class, derived from `Error` """
    pass

def make_ex(ret, msg):
    """
    Translate a librados return code into an exception.

    :param ret: the return code
    :type ret: int
    :param msg: the error message to use
    :type msg: str
    :returns: a subclass of :class:`Error`
    """

    errors = {
        errno.EPERM     : PermissionError,
        errno.ENOENT    : ObjectNotFound,
        errno.EIO       : IOError,
        errno.ENOSPC    : NoSpace,
        errno.EEXIST    : ObjectExists,
        errno.ENODATA   : NoData,
        errno.EINTR     : InterruptedOrTimeoutError,
        errno.ETIMEDOUT : TimedOut
        }
    ret = abs(ret)
    if ret in errors:
        return errors[ret](msg)
    else:
        return Error(msg + (": errno %s" % errno.errorcode[ret]))

class rados_cluster_stat_t(Structure):
    """ Cluster-wide usage information """
    _fields_ = [("kb", c_uint64),
                ("kb_used", c_uint64),
                ("kb_avail", c_uint64),
                ("num_objects", c_uint64)]

class Version(object):
    """ Version information """
    def __init__(self, major, minor, extra):
        self.major = major
        self.minor = minor
        self.extra = extra

    def __str__(self):
        return "%d.%d.%d" % (self.major, self.minor, self.extra)

class RadosThread(threading.Thread):
    def __init__(self, target, args=None):
        self.args = args
        self.target = target
        threading.Thread.__init__(self)

    def run(self):
        self.retval = self.target(*self.args)

# time in seconds between each call to t.join() for child thread
POLL_TIME_INCR = 0.5

def run_in_thread(target, args, timeout=0):
    import sys
    interrupt = False

    countdown = timeout
    t = RadosThread(target, args)

    # allow the main thread to exit (presumably, avoid a join() on this
    # subthread) before this thread terminates.  This allows SIGINT
    # exit of a blocked call.  See below.
    t.daemon = True

    t.start()
    try:
        # poll for thread exit
        while t.is_alive():
            t.join(POLL_TIME_INCR)
            if timeout:
                countdown = countdown - POLL_TIME_INCR
                if countdown <= 0:
                    raise KeyboardInterrupt

        t.join()        # in case t exits before reaching the join() above
    except KeyboardInterrupt:
        # ..but allow SIGINT to terminate the waiting.  Note: this
        # relies on the Linux kernel behavior of delivering the signal
        # to the main thread in preference to any subthread (all that's
        # strictly guaranteed is that *some* thread that has the signal
        # unblocked will receive it).  But there doesn't seem to be
        # any interface to create t with SIGINT blocked.
        interrupt = True

    if interrupt:
        t.retval = -errno.EINTR
    return t.retval

class Rados(object):
    """librados python wrapper"""

    def __init__(self, rados_id=None, name=None, clustername=None,
                 conf_defaults=None, conffile=None, conf=None, flags=0):
        librados_path = find_library('rados')
        if not librados_path:
            #maybe find_library can not find it correctly on all platforms.
            try:
                self.librados = CDLL('librados.so.2')
            except OSError as e:
                    raise EnvironmentError("Unable to load librados: %s" % e)
            except:
                raise Error("Unexpected error")
        else:
            self.librados = CDLL(librados_path)
        self.cluster = c_void_p()
        self.rados_id = rados_id
        if rados_id is not None and not isinstance(rados_id, str):
            raise TypeError('rados_id must be a string or None')
        if conffile is not None and not isinstance(conffile, str):
            raise TypeError('conffile must be a string or None')
        if name is not None and not isinstance(name, str):
            raise TypeError('name must be a string or None')
        if clustername is not None and not isinstance(clustername, str):
            raise TypeError('clustername must be a string or None')
        if rados_id and name:
            raise Error("Rados(): can't supply both rados_id and name")
        if rados_id:
            name = 'client.' +  rados_id
        if name is None:
            name = 'client.admin'
        if clustername is None:
            clustername = 'ceph'
        ret = run_in_thread(self.librados.rados_create2,
                            (byref(self.cluster), c_char_p(clustername),
                            c_char_p(name), c_uint64(flags)))

        if ret != 0:
            raise Error("rados_initialize failed with error code: %d" % ret)
        self.state = "configuring"
        # order is important: conf_defaults, then conffile, then conf
        if conf_defaults:
            for key, value in conf_defaults.iteritems():
                self.conf_set(key, value)
        if conffile is not None:
            # read the default conf file when '' is given
            if conffile == '':
                conffile = None
            self.conf_read_file(conffile)
        if conf:
            for key, value in conf.iteritems():
                self.conf_set(key, value)

    def __enter__(self):
        return self

    def __exit__(self, type_, value, traceback):
        return False

    def __del__(self):
        self.shutdown()

    def version(self):
        """
        Get the version number of the ``librados`` C library.

        :returns: a tuple of ``(major, minor, extra)`` components of the
                  librados version
        """
        major = c_int(0)
        minor = c_int(0)
        extra = c_int(0)
        run_in_thread(self.librados.rados_version,
                      (byref(major), byref(minor), byref(extra)))
        return Version(major.value, minor.value, extra.value)

    def conf_read_file(self, path=None):
        """
        Configure the cluster handle using a Ceph config file.

        :param path: path to the config file
        :type path: str
        """
        if path is not None and not isinstance(path, str):
            raise TypeError('path must be a string')
        ret = run_in_thread(self.librados.rados_conf_read_file,
                            (self.cluster, c_char_p(path)))
        if (ret != 0):
            raise make_ex(ret, "error calling conf_read_file")

    def conf_parse_argv(self, args):
        """
        Parse known arguments from args, and remove; returned
        args contain only those unknown to ceph
        """
        if not args:
            return
        # create instances of arrays of c_char_p's, both len(args) long
        # cretargs will always be a subset of cargs (perhaps identical)
        cargs = (c_char_p * len(args))(*args)
        cretargs = (c_char_p * len(args))()
        ret = run_in_thread(self.librados.rados_conf_parse_argv_remainder,
                            (self.cluster, len(args), cargs, cretargs))
        if ret:
            raise make_ex(ret, "error calling conf_parse_argv_remainder")

        # cretargs was allocated with fixed length; collapse return
        # list to eliminate any missing args
        return [a for a in cretargs if a is not None]

    def conf_parse_env(self, var='CEPH_ARGS'):
        """
        Parse known arguments from an environment variable, normally
        CEPH_ARGS.
        """
        if not var:
            return
        ret = run_in_thread(self.librados.rados_conf_parse_env,
                            (self.cluster, c_char_p(var)))
        if (ret != 0):
            raise make_ex(ret, "error calling conf_parse_env")

    def conf_get(self, option):
        """
        Get the value of a configuration option

        :param option: which option to read
        :type option: str

        :returns: str - value of the option or None
        :raises: :class:`TypeError`
        """
        if not isinstance(option, str):
            raise TypeError('option must be a string')
        length = 20
        while True:
            ret_buf = create_string_buffer(length)
            ret = run_in_thread(self.librados.rados_conf_get,
                                (self.cluster, c_char_p(option), ret_buf,
                                c_size_t(length)))
            if (ret == 0):
                return ret_buf.value
            elif (ret == -errno.ENAMETOOLONG):
                length = length * 2
            elif (ret == -errno.ENOENT):
                return None
            else:
                raise make_ex(ret, "error calling conf_get")

    def conf_set(self, option, val):
        """
        Set the value of a configuration option

        :param option: which option to set
        :type option: str
        :param option: value of the option
        :type option: str

        :raises: :class:`TypeError`, :class:`ObjectNotFound`
        """
        if not isinstance(option, str):
            raise TypeError('option must be a string')
        if not isinstance(val, str):
            raise TypeError('val must be a string')
        ret = run_in_thread(self.librados.rados_conf_set,
                            (self.cluster, c_char_p(option), c_char_p(val)))
        if (ret != 0):
            raise make_ex(ret, "error calling conf_set")


    def ping_monitor(self, mon_id):
      """
      Ping a monitor to assess liveness

      May be used as a simply way to assess liveness, or to obtain
      information about the monitor in a simple way even in the
      absence of quorum.

      :param mon_id: the ID portion of the monitor's name (i.e., mon.<ID>)
      :type mon_id: str
      :returns: the string reply from the monitor
      """

      outstrp = pointer(pointer(c_char()))
      outstrlen = c_long()

      ret = run_in_thread(self.librados.rados_ping_monitor,
                          (self.cluster, c_char_p(mon_id),
                           outstrp, byref(outstrlen)))

      my_outstr = outstrp.contents[:(outstrlen.value)]
      if outstrlen.value:
        run_in_thread(self.librados.rados_buffer_free, (outstrp.contents,))

      if ret != 0:
        raise make_ex(ret, "error calling ping_monitor")
      return my_outstr

    def get_cluster_stats(self):
        """
        Read usage info about the cluster

        This tells you total space, space used, space available, and number
        of objects. These are not updated immediately when data is written,
        they are eventually consistent.

        :returns: dict - contains the following keys:

            - ``kb`` (int) - total space

            - ``kb_used`` (int) - space used

            - ``kb_avail`` (int) - free space available

            - ``num_objects`` (int) - number of objects

        """
        stats = rados_cluster_stat_t()
        ret = run_in_thread(self.librados.rados_cluster_stat,
                            (self.cluster, byref(stats)))
        if ret < 0:
            raise make_ex(
                ret, "Rados.get_cluster_stats(%s): get_stats failed" % self.rados_id)
        return {'kb': stats.kb,
                'kb_used': stats.kb_used,
                'kb_avail': stats.kb_avail,
                'num_objects': stats.num_objects}

    def get_fsid(self):
        """
        Get the fsid of the cluster as a hexadecimal string.

        :raises: :class:`Error`
        :returns: str - cluster fsid
        """
        buf_len = 37
        fsid = create_string_buffer(buf_len)
        ret = run_in_thread(self.librados.rados_cluster_fsid,
                            (self.cluster, byref(fsid), c_size_t(buf_len)))
        if ret < 0:
            raise make_ex(ret, "error getting cluster fsid")
        return fsid.value

    def open_ioctx(self, ioctx_name):
        """
        Create an io context

        The io context allows you to perform operations within a particular
        volume.

        :param ioctx_name: name of the volume
        :type ioctx_name: str

        :raises: :class:`TypeError`, :class:`Error`
        :returns: Ioctx - Rados Ioctx object
        """
        if not isinstance(ioctx_name, str):
            raise TypeError('ioctx_name must be a string')
        ioctx = c_void_p()
        ret = run_in_thread(self.librados.rados_ioctx_create,
                            (self.cluster, c_char_p(ioctx_name), byref(ioctx)))
        if ret < 0:
            raise make_ex(ret, "error opening ioctx '%s'" % ioctx_name)
        return Ioctx(ioctx_name, self.librados, ioctx)

    def mon_command(self, cmd, inbuf, timeout=0, target=None):
        """
        mon_command[_target](cmd, inbuf, outbuf, outbuflen, outs, outslen)
        returns (int ret, string outbuf, string outs)
        """
        import sys
        outbufp = pointer(pointer(c_char()))
        outbuflen = c_long()
        outsp = pointer(pointer(c_char()))
        outslen = c_long()
        cmdarr = (c_char_p * len(cmd))(*cmd)

        if target:
            ret = run_in_thread(self.librados.rados_mon_command_target,
                                (self.cluster, c_char_p(target), cmdarr,
                                 len(cmd), c_char_p(inbuf), len(inbuf),
                                 outbufp, byref(outbuflen), outsp,
                                 byref(outslen)), timeout)
        else:
            ret = run_in_thread(self.librados.rados_mon_command,
                                (self.cluster, cmdarr, len(cmd),
                                 c_char_p(inbuf), len(inbuf),
                                 outbufp, byref(outbuflen), outsp, byref(outslen)),
                                timeout)

        # copy returned memory (ctypes makes a copy, not a reference)
        my_outbuf = outbufp.contents[:(outbuflen.value)]
        my_outs = outsp.contents[:(outslen.value)]

        # free callee's allocations
        if outbuflen.value:
            run_in_thread(self.librados.rados_buffer_free, (outbufp.contents,))
        if outslen.value:
            run_in_thread(self.librados.rados_buffer_free, (outsp.contents,))

        return (ret, my_outbuf, my_outs)

class XattrIterator(object):
    """Extended attribute iterator"""
    def __init__(self, ioctx, it, oid):
        self.ioctx = ioctx
        self.it = it
        self.oid = oid

    def __iter__(self):
        return self

    def next(self):
        """
        Get the next xattr on the object

        :raises: StopIteration
        :returns: pair - of name and value of the next Xattr
        """
        name_ = c_char_p(0)
        val_ = c_char_p(0)
        len_ = c_int(0)
        ret = run_in_thread(self.ioctx.librados.rados_getxattrs_next,
                            (self.it, byref(name_), byref(val_), byref(len_)))
        if (ret != 0):
            raise make_ex(ret, "error iterating over the extended attributes \
in '%s'" % self.oid)
        if name_.value == None:
            raise StopIteration()
        name = ctypes.string_at(name_)
        val = ctypes.string_at(val_, len_)
        return (name, val)

    def __del__(self):
        run_in_thread(self.ioctx.librados.rados_getxattrs_end, (self.it,))

class Completion(object):
    """completion object"""
    def __init__(self, ioctx, rados_comp, oncomplete, onsafe):
        self.rados_comp = rados_comp
        self.oncomplete = oncomplete
        self.onsafe = onsafe
        self.ioctx = ioctx

    def wait_for_safe(self):
        """
        Is an asynchronous operation safe?

        This does not imply that the safe callback has finished.

        :returns: whether the operation is safe
        """
        return run_in_thread(self.ioctx.librados.rados_aio_is_safe,
                             (self.rados_comp,))

    def wait_for_complete(self):
        """
        Has an asynchronous operation completed?

        This does not imply that the safe callback has finished.

        :returns:  whether the operation is completed
        """
        return run_in_thread(self.ioctx.librados.rados_aio_is_complete,
                             (self.rados_comp,))

    def get_return_value(self):
        """
        Get the return value of an asychronous operation

        The return value is set when the operation is complete or safe,
        whichever comes first.

        :returns: int - return value of the operation
        """
        return run_in_thread(self.ioctx.librados.rados_aio_get_return_value,
                             (self.rados_comp,))

    def __del__(self):
        """
        Release a completion

        Call this when you no longer need the completion. It may not be
        freed immediately if the operation is not acked and committed.
        """
        run_in_thread(self.ioctx.librados.rados_aio_release,
                      (self.rados_comp,))

class Ioctx(object):
    """rados.Ioctx object"""
    def __init__(self, name, librados, io):
        self.name = name
        self.librados = librados
        self.io = io
        self.state = "open"
        self.safe_cbs = {}
        self.complete_cbs = {}
        RADOS_CB = CFUNCTYPE(c_int, c_void_p, c_void_p)
        self.__aio_safe_cb_c = RADOS_CB(self.__aio_safe_cb)
        self.__aio_complete_cb_c = RADOS_CB(self.__aio_complete_cb)
        self.lock = threading.Lock()

    def __enter__(self):
        return self

    def __exit__(self, type_, value, traceback):
        self.close()
        return False

    def __del__(self):
        self.close()

    def __aio_safe_cb(self, completion, _):
        """
        Callback to onsafe() for asynchronous operations
        """
        cb = None
        with self.lock:
            cb = self.safe_cbs[completion]
            del self.safe_cbs[completion]
        cb.onsafe(cb)
        return 0

    def __aio_complete_cb(self, completion, _):
        """
        Callback to oncomplete() for asynchronous operations
        """
        cb = None
        with self.lock:
            cb = self.complete_cbs[completion]
            del self.complete_cbs[completion]
        cb.oncomplete(cb)
        return 0

    def __get_completion(self, oncomplete, onsafe):
        """
        Constructs a completion to use with asynchronous operations

        :param oncomplete: what to do when the write is safe and complete in memory
            on all replicas
        :type oncomplete: completion
        :param onsafe:  what to do when the write is safe and complete on storage
            on all replicas
        :type onsafe: completion

        :raises: :class:`Error`
        :returns: completion object
        """
        completion = c_void_p(0)
        complete_cb = None
        safe_cb = None
        if oncomplete:
            complete_cb = self.__aio_complete_cb_c
        if onsafe:
            safe_cb = self.__aio_safe_cb_c
        ret = run_in_thread(self.librados.rados_aio_create_completion,
                            (c_void_p(0), complete_cb, safe_cb,
                            byref(completion)))
        if ret < 0:
            raise make_ex(ret, "error getting a completion")
        with self.lock:
            completion_obj = Completion(self, completion, oncomplete, onsafe)
            if oncomplete:
                self.complete_cbs[completion.value] = completion_obj
            if onsafe:
                self.safe_cbs[completion.value] = completion_obj
        return completion_obj

    def aio_write(self, object_name, to_write, offset=0,
                  oncomplete=None, onsafe=None):
        """
        Write data to an object asynchronously

        Queues the write and returns.

        :param object_name: name of the object
        :type object_name: str
        :param to_write: data to write
        :type to_write: str
        :param offset: byte offset in the object to begin writing at
        :type offset: int
        :param oncomplete: what to do when the write is safe and complete in memory
            on all replicas
        :type oncomplete: completion
        :param onsafe:  what to do when the write is safe and complete on storage
            on all replicas
        :type onsafe: completion

        :raises: :class:`Error`
        :returns: completion object
        """
        completion = self.__get_completion(oncomplete, onsafe)
        ret = run_in_thread(self.librados.rados_aio_write,
                            (self.io, c_char_p(object_name),
                            completion.rados_comp, c_char_p(to_write),
                            c_size_t(len(to_write)), c_uint64(offset)))
        if ret < 0:
            raise make_ex(ret, "error writing object %s" % object_name)
        return completion

    def aio_write_full(self, object_name, to_write,
                       oncomplete=None, onsafe=None):
        """
        Asychronously write an entire object

        The object is filled with the provided data. If the object exists,
        it is atomically truncated and then written.
        Queues the write and returns.

        :param object_name: name of the object
        :type object_name: str
        :param to_write: data to write
        :type to_write: str
        :param oncomplete: what to do when the write is safe and complete in memory
            on all replicas
        :type oncomplete: completion
        :param onsafe:  what to do when the write is safe and complete on storage
            on all replicas
        :type onsafe: completion

        :raises: :class:`Error`
        :returns: completion object
        """
        completion = self.__get_completion(oncomplete, onsafe)
        ret = run_in_thread(self.librados.rados_aio_write_full,
                            (self.io, c_char_p(object_name),
                            completion.rados_comp, c_char_p(to_write),
                            c_size_t(len(to_write))))
        if ret < 0:
            raise make_ex(ret, "error writing object %s" % object_name)
        return completion

    def aio_append(self, object_name, to_append, oncomplete=None, onsafe=None):
        """
        Asychronously append data to an object

        Queues the write and returns.

        :param object_name: name of the object
        :type object_name: str
        :param to_append: data to append
        :type to_append: str
        :param offset: byte offset in the object to begin writing at
        :type offset: int
        :param oncomplete: what to do when the write is safe and complete in memory
            on all replicas
        :type oncomplete: completion
        :param onsafe:  what to do when the write is safe and complete on storage
            on all replicas
        :type onsafe: completion

        :raises: :class:`Error`
        :returns: completion object
        """
        completion = self.__get_completion(oncomplete, onsafe)
        ret = run_in_thread(self.librados.rados_aio_append,
                            (self.io, c_char_p(object_name),
                            completion.rados_comp, c_char_p(to_append),
                            c_size_t(len(to_append))))
        if ret < 0:
            raise make_ex(ret, "error appending to object %s" % object_name)
        return completion

    def aio_flush(self):
        """
        Block until all pending writes in an io context are safe

        :raises: :class:`Error`
        """
        ret = run_in_thread(self.librados.rados_aio_flush, (self.io,))
        if ret < 0:
            raise make_ex(ret, "error flushing")

    def aio_read(self, object_name, length, offset, oncomplete):
        """
        Asychronously read data from an object

        oncomplete will be called with the returned read value as
        well as the completion:

        oncomplete(completion, data_read)

        :param object_name: name of the object to read from
        :type object_name: str
        :param length: the number of bytes to read
        :type length: int
        :param offset: byte offset in the object to begin reading from
        :type offset: int
        :param oncomplete: what to do when the read is complete
        :type oncomplete: completion

        :raises: :class:`Error`
        :returns: completion object
        """
        buf = create_string_buffer(length)
        def oncomplete_(completion):
            return oncomplete(completion, buf.value)
        completion = self.__get_completion(oncomplete_, None)
        ret = run_in_thread(self.librados.rados_aio_read,
                            (self.io, c_char_p(object_name),
                            completion.rados_comp, buf, c_size_t(length),
                            c_uint64(offset)))
        if ret < 0:
            raise make_ex(ret, "error reading %s" % object_name)
        return completion

    def aio_remove(self, object_name, oncomplete=None, onsafe=None):
        """
        Asychronously remove an object

        :param object_name: name of the object to remove
        :type object_name: str
        :param oncomplete: what to do when the remove is safe and complete in memory
            on all replicas
        :type oncomplete: completion
        :param onsafe:  what to do when the remove is safe and complete on storage
            on all replicas
        :type onsafe: completion

        :raises: :class:`Error`
        :returns: completion object
        """
        completion = self.__get_completion(oncomplete, onsafe)
        ret = run_in_thread(self.librados.rados_aio_remove,
                            (self.io, c_char_p(object_name),
                            completion.rados_comp))
        if ret < 0:
            raise make_ex(ret, "error removing %s" % object_name)
        return completion

    def require_ioctx_open(self):
        """
        Checks if the rados.Ioctx object state is 'open'

        :raises: IoctxStateError
        """
        if self.state != "open":
            raise IoctxStateError("The volume is %s" % self.state)

    def close(self):
        """
        Close a rados.Ioctx object.

        This just tells librados that you no longer need to use the io context.
        It may not be freed immediately if there are pending asynchronous
        requests on it, but you should not use an io context again after
        calling this function on it.
        """
        if self.state == "open":
            self.require_ioctx_open()
            run_in_thread(self.librados.rados_ioctx_destroy, (self.io,))
            self.state = "closed"

    def write(self, key, data, offset=0):
        """
        Write data to an object synchronously

        :param key: name of the object
        :type key: str
        :param data: data to write
        :type data: str
        :param offset: byte offset in the object to begin writing at
        :type offset: int

        :raises: :class:`TypeError`
        :raises: :class:`LogicError`
        :returns: int - number of bytes written
        """
        self.require_ioctx_open()
        if not isinstance(key, str):
            raise TypeError('key must be a string')
        if not isinstance(data, str):
            raise TypeError('data must be a string')
        length = len(data)
        ret = run_in_thread(self.librados.rados_write,
                            (self.io, c_char_p(key), c_char_p(data),
                            c_size_t(length), c_uint64(offset)))
        if ret == 0:
            return ret
        elif ret < 0:
            raise make_ex(ret, "Ioctx.write(%s): failed to write %s" % \
                (self.name, key))
        else:
            raise LogicError("Ioctx.write(%s): rados_write \
returned %d, but should return zero on success." % (self.name, ret))

    def write_full(self, key, data):
        """
        Write an entire object synchronously.

        The object is filled with the provided data. If the object exists,
        it is atomically truncated and then written.

        :param key: name of the object
        :type key: str
        :param data: data to write
        :type data: str

        :raises: :class:`TypeError`
        :raises: :class:`Error`
        :returns: int - 0 on success
        """
        self.require_ioctx_open()
        if not isinstance(key, str):
            raise TypeError('key must be a string')
        if not isinstance(data, str):
            raise TypeError('data must be a string')
        length = len(data)
        ret = run_in_thread(self.librados.rados_write_full,
                            (self.io, c_char_p(key), c_char_p(data),
                            c_size_t(length)))
        if ret == 0:
            return ret
        elif ret < 0:
            raise make_ex(ret, "Ioctx.write_full(%s): failed to write %s" % \
                (self.name, key))
        else:
            raise LogicError("Ioctx.write_full(%s): rados_write_full \
returned %d, but should return zero on success." % (self.name, ret))

    def append(self, key, data):
        """
        Append data to an object synchronously

        :param key: name of the object
        :type key: str
        :param data: data to write
        :type data: str

        :raises: :class:`TypeError`
        :raises: :class:`LogicError`
        :returns: int - number of bytes written
        """
        self.require_ioctx_open()
        if not isinstance(key, str):
            raise TypeError('key must be a string')
        if not isinstance(data, str):
            raise TypeError('data must be a string')
        length = len(data)
        ret = run_in_thread(self.librados.rados_append,
                            (self.io, c_char_p(key), c_char_p(data),
                            c_size_t(length)))
        if ret == 0:
            return ret
        elif ret < 0:
            raise make_ex(ret, "Ioctx.append(%s): failed to append %s" % \
                (self.name, key))
        else:
            raise LogicError("Ioctx.append(%s): rados_append \
returned %d, but should return zero on success." % (self.name, ret))

    def read(self, key, length=8192, offset=0):
        """
        Read data from an object synchronously

        :param key: name of the object
        :type key: str
        :param length: the number of bytes to read (default=8192)
        :type length: int
        :param offset: byte offset in the object to begin reading at
        :type offset: int

        :raises: :class:`TypeError`
        :raises: :class:`Error`
        :returns: str - data read from object
        """
        self.require_ioctx_open()
        if not isinstance(key, str):
            raise TypeError('key must be a string')
        ret_buf = create_string_buffer(length)
        ret = run_in_thread(self.librados.rados_read,
                            (self.io, c_char_p(key), ret_buf, c_size_t(length),
                            c_uint64(offset)))
        if ret < 0:
            raise make_ex(ret, "Ioctx.read(%s): failed to read %s" % (self.name, key))
        return ctypes.string_at(ret_buf, ret)

    def remove_object(self, key):
        """
        Delete an object

        :param key: the name of the object to delete
        :type key: str

        :raises: :class:`TypeError`
        :raises: :class:`Error`
        :returns: bool - True on success
        """
        self.require_ioctx_open()
        if not isinstance(key, str):
            raise TypeError('key must be a string')
        ret = run_in_thread(self.librados.rados_remove,
                            (self.io, c_char_p(key)))
        if ret < 0:
            raise make_ex(ret, "Failed to remove '%s'" % key)
        return True

    def trunc(self, key, size):
        """
        Resize an object

        If this enlarges the object, the new area is logically filled with
        zeroes. If this shrinks the object, the excess data is removed.

        :param key: the name of the object to resize
        :type key: str
        :param size: the new size of the object in bytes
        :type size: int

        :raises: :class:`TypeError`
        :raises: :class:`Error`
        :returns: int - 0 on success, otherwise raises error
        """

        self.require_ioctx_open()
        if not isinstance(key, str):
            raise TypeError('key must be a string')
        ret = run_in_thread(self.librados.rados_trunc,
                            (self.io, c_char_p(key), c_uint64(size)))
        if ret < 0:
            raise make_ex(ret, "Ioctx.trunc(%s): failed to truncate %s" % (self.name, key))
        return ret

    def stat(self, key):
        """
        Get object stats (size/mtime)

        :param key: the name of the object to get stats from
        :type key: str

        :raises: :class:`TypeError`
        :raises: :class:`Error`
        :returns: (size,timestamp)
        """
        self.require_ioctx_open()
        if not isinstance(key, str):
            raise TypeError('key must be a string')
        psize = c_uint64()
        pmtime = c_uint64()

        ret = run_in_thread(self.librados.rados_stat,
                            (self.io, c_char_p(key), pointer(psize),
                            pointer(pmtime)))
        if ret < 0:
            raise make_ex(ret, "Failed to stat %r" % key)
        return psize.value, time.localtime(pmtime.value)

    def get_xattr(self, key, xattr_name):
        """
        Get the value of an extended attribute on an object.

        :param key: the name of the object to get xattr from
        :type key: str
        :param xattr_name: which extended attribute to read
        :type xattr_name: str

        :raises: :class:`TypeError`
        :raises: :class:`Error`
        :returns: str - value of the xattr
        """
        self.require_ioctx_open()
        if not isinstance(xattr_name, str):
            raise TypeError('xattr_name must be a string')
        ret_length = 4096
        while ret_length < 4096 * 1024 * 1024:
            ret_buf = create_string_buffer(ret_length)
            ret = run_in_thread(self.librados.rados_getxattr,
                                (self.io, c_char_p(key), c_char_p(xattr_name),
                                 ret_buf, c_size_t(ret_length)))
            if (ret == -errno.ERANGE):
                ret_length *= 2
            elif ret < 0:
                raise make_ex(ret, "Failed to get xattr %r" % xattr_name)
            else:
                break
        return ctypes.string_at(ret_buf, ret)

    def get_xattrs(self, oid):
        """
        Start iterating over xattrs on an object.

        :param oid: the name of the object to get xattrs from
        :type key: str

        :raises: :class:`TypeError`
        :raises: :class:`Error`
        :returns: XattrIterator
        """
        self.require_ioctx_open()
        if not isinstance(oid, str):
            raise TypeError('oid must be a string')
        it = c_void_p(0)
        ret = run_in_thread(self.librados.rados_getxattrs,
                            (self.io, oid, byref(it)))
        if ret != 0:
            raise make_ex(ret, "Failed to get rados xattrs for object %r" % oid)
        return XattrIterator(self, it, oid)

    def set_xattr(self, key, xattr_name, xattr_value):
        """
        Set an extended attribute on an object.

        :param key: the name of the object to set xattr to
        :type key: str
        :param xattr_name: which extended attribute to set
        :type xattr_name: str
        :param xattr_value: the value of the  extended attribute
        :type xattr_value: str

        :raises: :class:`TypeError`
        :raises: :class:`Error`
        :returns: bool - True on success, otherwise raise an error
        """
        self.require_ioctx_open()
        if not isinstance(key, str):
            raise TypeError('key must be a string')
        if not isinstance(xattr_name, str):
            raise TypeError('xattr_name must be a string')
        if not isinstance(xattr_value, str):
            raise TypeError('xattr_value must be a string')
        ret = run_in_thread(self.librados.rados_setxattr,
                            (self.io, c_char_p(key), c_char_p(xattr_name),
                            c_char_p(xattr_value), c_size_t(len(xattr_value))))
        if ret < 0:
            raise make_ex(ret, "Failed to set xattr %r" % xattr_name)
        return True

    def rm_xattr(self, key, xattr_name):
        """
        Removes an extended attribute on from an object.

        :param key: the name of the object to remove xattr from
        :type key: str
        :param xattr_name: which extended attribute to remove
        :type xattr_name: str

        :raises: :class:`TypeError`
        :raises: :class:`Error`
        :returns: bool - True on success, otherwise raise an error
        """
        self.require_ioctx_open()
        if not isinstance(key, str):
            raise TypeError('key must be a string')
        if not isinstance(xattr_name, str):
            raise TypeError('xattr_name must be a string')
        ret = run_in_thread(self.librados.rados_rmxattr,
                            (self.io, c_char_p(key), c_char_p(xattr_name)))
        if ret < 0:
            raise make_ex(ret, "Failed to delete key %r xattr %r" %
                (key, xattr_name))
        return True

    def get_last_version(self):
        """
        Return the version of the last object read or written to.

        This exposes the internal version number of the last object read or
        written via this io context

        :returns: version of the last object used
        """
        self.require_ioctx_open()
        return run_in_thread(self.librados.rados_get_last_version, (self.io,))

class Object(object):
    """Rados object wrapper, makes the object look like a file"""
    def __init__(self, ioctx, key):
        self.key = key
        self.ioctx = ioctx
        self.offset = 0
        self.state = "exists"

    def __str__(self):
        return "rados.Object(ioctx=%s,key=%s)" % (str(self.ioctx), self.key)

    def require_object_exists(self):
        if self.state != "exists":
            raise ObjectStateError("The object is %s" % self.state)

    def read(self, length = 1024*1024):
        self.require_object_exists()
        ret = self.ioctx.read(self.key, length, self.offset)
        self.offset += len(ret)
        return ret

    def write(self, string_to_write):
        self.require_object_exists()
        ret = self.ioctx.write(self.key, string_to_write, self.offset)
        self.offset += ret
        return ret

    def remove(self):
        self.require_object_exists()
        self.ioctx.remove_object(self.key)
        self.state = "removed"

    def stat(self):
        self.require_object_exists()
        return self.ioctx.stat(self.key)

    def seek(self, position):
        self.require_object_exists()
        self.offset = position

    def get_xattr(self, xattr_name):
        self.require_object_exists()
        return self.ioctx.get_xattr(self.key, xattr_name)

    def get_xattrs(self):
        self.require_object_exists()
        return self.ioctx.get_xattrs(self.key)

    def set_xattr(self, xattr_name, xattr_value):
        self.require_object_exists()
        return self.ioctx.set_xattr(self.key, xattr_name, xattr_value)

    def rm_xattr(self, xattr_name):
        self.require_object_exists()
        return self.ioctx.rm_xattr(self.key, xattr_name)

MONITOR_LEVELS = [
   "debug",
   "info",
   "warn", "warning",
   "err", "error",
   "sec",
   ]


class MonitorLog(object):
    """
    For watching cluster log messages.  Instantiate an object and keep
    it around while callback is periodically called.  Construct with
    'level' to monitor 'level' messages (one of MONITOR_LEVELS).
    arg will be passed to the callback.

    callback will be called with:
        arg (given to __init__)
        line (the full line, including timestamp, who, level, msg)
        who (which entity issued the log message)
        timestamp_sec (sec of a struct timespec)
        timestamp_nsec (sec of a struct timespec)
        seq (sequence number)
        level (string representing the level of the log message)
        msg (the message itself)
    callback's return value is ignored
    """

    def monitor_log_callback(self, arg, line, who, sec, nsec, seq, level, msg):
        """
        Local callback wrapper, in case we decide to do something
        """
        self.callback(arg, line, who, sec, nsec, seq, level, msg)
        return 0

    def __init__(self, cluster, level, callback, arg):
        if level not in MONITOR_LEVELS:
            raise LogicError("invalid monitor level " + level)
        if not callable(callback):
            raise LogicError("callback must be a callable function")
        self.level = level
        self.callback = callback
        self.arg = arg
        callback_factory = CFUNCTYPE(c_int,    # return type (really void)
                                     c_void_p, # arg
                                     c_char_p, # line
                                     c_char_p, # who
                                     c_uint64, # timestamp_sec
                                     c_uint64, # timestamp_nsec
                                     c_ulong,  # seq
                                     c_char_p, # level
                                     c_char_p) # msg
        self.internal_callback = callback_factory(self.monitor_log_callback)

        r = run_in_thread(cluster.librados.rados_monitor_log,
                          (cluster.cluster, level, self.internal_callback, arg))
        if r:
            raise make_ex(r, 'error calling rados_monitor_log')
