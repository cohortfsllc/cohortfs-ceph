// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
#ifndef __LIBRADOS_HPP
#define __LIBRADOS_HPP

#include <boost/uuid/uuid.hpp>
#include <stdbool.h>
#include <memory>
#include <string>
#include <list>
#include <map>
#include <set>
#include <vector>
#include <utility>
#include "buffer.h"

#include "librados.h"
#include "rados_types.hpp"
#include "include/Context.h"
#include "osdc/ObjectOperation.h"
#include "osdc/Objecter.h"
#include "vol/Volume.h"

namespace librados
{
  using ceph::bufferlist;

  class RadosClient;
  struct AioCompletionImpl;
  class IoCtx;
  struct IoCtxImpl;

  typedef void *config_t;

  struct cluster_stat_t {
    uint64_t kb, kb_used, kb_avail;
    uint64_t num_objects;
  };

  typedef struct {
    std::string client;
    std::string cookie;
    std::string address;
  } locker_t;

  typedef void *completion_t;
  typedef void (*callback_t)(completion_t cb, void *arg);

  class WatchCtx {
  public:
    virtual ~WatchCtx();
    virtual void notify(uint8_t opcode, uint64_t ver, bufferlist& bl) = 0;
  };

  struct AioCompletion {
    AioCompletion(AioCompletionImpl *pc_) : pc(pc_) {}
    int set_complete_callback(void *cb_arg, callback_t cb);
    int set_safe_callback(void *cb_arg, callback_t cb);
    int wait_for_complete();
    int wait_for_safe();
    int wait_for_complete_and_cb();
    int wait_for_safe_and_cb();
    bool is_complete();
    bool is_safe();
    bool is_complete_and_cb();
    bool is_safe_and_cb();
    int get_return_value();
    uint64_t get_version();
    void release();
    AioCompletionImpl *pc;
  };

  /**
   * These are per-op flags which may be different among
   * ops added to an ObjectOperation.
   */
  enum ObjectOperationFlags {
    OP_EXCL =	LIBRADOS_OP_FLAG_EXCL,
    OP_FAILOK = LIBRADOS_OP_FLAG_FAILOK,
  };

  class ObjectOperationCompletion {
  public:
    virtual ~ObjectOperationCompletion() {}
    virtual void handle_completion(int r, bufferlist& outbl) = 0;
  };

  /**
   * These flags apply to the ObjectOperation as a whole.
   *
   * BALANCE_READS and LOCALIZE_READS should only be used
   * when reading from data you're certain won't change
   * or where eventual consistency is ok.
   *
   * ORDER_READS_WRITES will order reads the same way writes are
   * ordered (e.g., waiting for degraded objects).  In particular, it
   * will make a write followed by a read sequence be preserved.
   */
  enum ObjectOperationGlobalFlags {
    OPERATION_NOFLAG = 0,
    OPERATION_BALANCE_READS  = 1,
    OPERATION_LOCALIZE_READS = 2,
    OPERATION_ORDER_READS_WRITES = 4,
    OPERATION_SKIPRWLOCKS = 16,
  };

  /*
   * ObjectOperation : compound object operation
   * Batch multiple object operations into a single request, to be applied
   * atomically.
   */
  class ObjectOperation
  {
  public:
    ObjectOperation(const IoCtx& ctx);
    ObjectOperation(const ObjectOperation& other) :
      impl(other.impl->clone()) { }
    ObjectOperation(ObjectOperation&& other) {
      swap(impl, other.impl);
    }
    ObjectOperation& operator= (const ObjectOperation& rhs) {
      impl = rhs.impl->clone();
      return *this;
    }


    virtual ~ObjectOperation() = default;

    size_t size();
    void set_op_flags(ObjectOperationFlags flags);

    void cmpxattr(const char *name, uint8_t op, const bufferlist& val);
    void cmpxattr(const char *name, uint8_t op, uint64_t v);
    void src_cmpxattr(const std::string& src_oid,
		      const char *name, int op, const bufferlist& val);
    void src_cmpxattr(const std::string& src_oid,
		      const char *name, int op, uint64_t v);
    void exec(const char *cls, const char *method, bufferlist& inbl);
    void exec(const char *cls, const char *method, bufferlist& inbl,
	      bufferlist *obl, Context *ctx, int *prval);
    void exec(const char *cls, const char *method, bufferlist& inbl,
	      bufferlist *obl, int *prval);
    void exec(const char *cls, const char *method, bufferlist& inbl,
	      ObjectOperationCompletion *completion);
    /**
     * Guard operatation with a check that the object already exists
     */
    void assert_exists();

    /**
     * get key/value pairs for specified keys
     *
     * @param assertions [in] comparison assertions
     * @param prval [out] place error code in prval upon completion
     *
     * assertions has the form of mappings from keys to (comparison rval, assertion)
     * The assertion field may be CEPH_OSD_CMPXATTR_OP_[GT|LT|EQ].
     *
     * That is, to assert that the value at key 'foo' is greater than 'bar':
     *
     * ObjectReadOperation op;
     * int r;
     * map<string, pair<bufferlist, int> > assertions;
     * bufferlist bar(string('bar'));
     * assertions['foo'] = make_pair(bar, CEPH_OSD_CMP_XATTR_OP_GT);
     * op.omap_cmp(assertions, &r);
     */
    void omap_cmp(
      const std::map<std::string, std::pair<bufferlist, int> > &assertions,
      int *prval);

  protected:
    std::unique_ptr<ObjOp> impl;
    friend class IoCtx;
    friend class Rados;
    friend struct IoCtxImpl;
  };

  /*
   * ObjectWriteOperation : compound object write operation
   * Batch multiple object operations into a single request, to be applied
   * atomically.
   */
  class ObjectWriteOperation : public ObjectOperation
  {
  protected:
    time_t *pmtime;
  public:
    ObjectWriteOperation(const IoCtx &ctx)
      : ObjectOperation(ctx), pmtime(NULL) {}
    ~ObjectWriteOperation() {}

    void mtime(time_t *pt) {
      pmtime = pt;
    }

    void create(bool exclusive);
    void create(bool exclusive, const string& category);
    void write(uint64_t off, const bufferlist& bl);
    void write_full(const bufferlist& bl);
    void append(const bufferlist& bl);
    void remove();
    void truncate(uint64_t off);
    void zero(uint64_t off, uint64_t len);
    void rmxattr(const char *name);
    void setxattr(const char *name, const bufferlist& bl);

    /**
     * set keys and values according to map
     *
     * @param map [in] keys and values to set
     */
    void omap_set(const std::map<std::string, bufferlist> &map);

    /**
     * set header
     *
     * @param bl [in] header to set
     */
    void omap_set_header(const bufferlist &bl);

    /**
     * Clears omap contents
     */
    void omap_clear();

    /**
     * Clears keys in to_rm
     *
     * @param to_rm [in] keys to remove
     */
    void omap_rm_keys(const std::set<std::string> &to_rm);

    /**
     * Set allocation hint for an object
     *
     * @param expected_object_size expected size of the object, in bytes
     * @param expected_write_size expected size of writes to the object, in bytes
     */
    void set_alloc_hint(uint64_t expected_object_size,
			uint64_t expected_write_size);

    friend class IoCtx;
  };

  /*
   * ObjectReadOperation : compound object operation that return value
   * Batch multiple object operations into a single request, to be applied
   * atomically.
   */
  class ObjectReadOperation : public ObjectOperation
  {
  public:
    ObjectReadOperation(const IoCtx& ctx) : ObjectOperation(ctx) {}
    ~ObjectReadOperation() {}

    void stat(uint64_t *psize, time_t *pmtime, int *prval);
    void getxattr(const char *name, bufferlist *pbl, int *prval);
    void getxattrs(std::map<std::string, bufferlist> &pattrs, int *prval);
    void read(size_t off, uint64_t len, bufferlist *pbl, int *prval);
    void read(size_t off, uint64_t len, bufferlist *pbl, int *prval,
	      Context *ctx);
    /**
     * see aio_sparse_read()
     */
    void sparse_read(uint64_t off, uint64_t len, std::map<uint64_t,uint64_t> *m,
		     bufferlist *data_bl, int *prval);

    /**
     * omap_get_vals: keys and values from the object omap
     *
     * Get up to max_return keys and values beginning after start_after
     *
     * @param start_after [in] list no keys smaller than start_after
     * @parem max_return [in] list no more than max_return key/value pairs
     * @param out_vals [out] place returned values in out_vals on completion
     * @param prval [out] place error code in prval upon completion
     */
    void omap_get_vals(
      const std::string &start_after,
      uint64_t max_return,
      std::map<std::string, bufferlist> &out_vals,
      int *prval);

    /**
     * omap_get_vals: keys and values from the object omap
     *
     * Get up to max_return keys and values beginning after start_after
     *
     * @param start_after [in] list keys starting after start_after
     * @param filter_prefix [in] list only keys beginning with filter_prefix
     * @parem max_return [in] list no more than max_return key/value pairs
     * @param out_vals [out] place returned values in out_vals on completion
     * @param prval [out] place error code in prval upon completion
     */
    void omap_get_vals(
      const std::string &start_after,
      const std::string &filter_prefix,
      uint64_t max_return,
      std::map<std::string, bufferlist> &out_vals,
      int *prval);


    /**
     * omap_get_keys: keys from the object omap
     *
     * Get up to max_return keys beginning after start_after
     *
     * @param start_after [in] list keys starting after start_after
     * @parem max_return [in] list no more than max_return keys
     * @param out_keys [out] place returned values in out_keys on completion
     * @param prval [out] place error code in prval upon completion
     */
    void omap_get_keys(const std::string &start_after,
		       uint64_t max_return,
		       std::set<std::string> *out_keys,
		       int *prval);

    /**
     * omap_get_header: get header from object omap
     *
     * @param header [out] place header here upon completion
     * @param prval [out] place error code in prval upon completion
     */
    void omap_get_header(bufferlist *header, int *prval);

    /**
     * get key/value pairs for specified keys
     *
     * @param to_get [in] keys to get
     * @param out_vals [out] place key/value pairs found here on completion
     * @param prval [out] place error code in prval upon completion
     */
    void omap_get_vals_by_keys(const std::set<std::string> &keys,
			       std::map<std::string, bufferlist> &map,
			       int *prval);

    /**
     * list_watchers: Get list watchers of object
     *
     * @param out_watchers [out] place returned values in out_watchers on completion
     * @param prval [out] place error code in prval upon completion
     */
    void list_watchers(std::list<obj_watch_t> *out_watchers, int *prval);
  };

  /* IoCtx : This is a context in which we can perform I/O.
   *
   * Typical use (error checking omitted):
   *
   * IoCtx p;
   * rados.ioctx_create("my_volume", p);
   * p->stat(&stats);
   * ... etc ...
   */
  class IoCtx
  {
  public:
    IoCtx();
    static void from_rados_ioctx_t(rados_ioctx_t p, IoCtx &ctx);
    IoCtx(const IoCtx& rhs);
    IoCtx& operator=(const IoCtx& rhs);

    ~IoCtx();

    void close();

    // deep copy
    void dup(const IoCtx& rhs);

    const std::string& get_volume_name();
    const boost::uuids::uuid& get_volume_id();

    // create an object
    int create(const std::string& oid_t, bool exclusive);

    /**
     * write bytes to an object at a specified offset
     *
     * NOTE: this call steals the contents of @param bl.
     */
    int write(const std::string& oid_t, bufferlist& bl, size_t len, uint64_t off);
    /**
     * append bytes to an object
     *
     * NOTE: this call steals the contents of @param bl.
     */
    int append(const std::string& oid_t, bufferlist& bl, size_t len);
    /**
     * replace object contents with provided data
     *
     * NOTE: this call steals the contents of @param bl.
     */
    int write_full(const std::string& oid_t, bufferlist& bl);
    int read(const std::string& oid_t, bufferlist& bl, size_t len, uint64_t off);
    int remove(const std::string& oid_t);
    int trunc(const std::string& oid_t, uint64_t size);
    int sparse_read(const std::string& o, std::map<uint64_t,uint64_t>& m, bufferlist& bl, size_t len, uint64_t off);
    int getxattr(const std::string& oid_t, const char *name, bufferlist& bl);
    int getxattrs(const std::string& oid_t, std::map<std::string, bufferlist>& attrset);
    int setxattr(const std::string& oid_t, const char *name, bufferlist& bl);
    int rmxattr(const std::string& oid_t, const char *name);
    uint64_t op_size(void);
    int stat(const std::string& oid_t, uint64_t *psize, time_t *pmtime);
    int exec(const std::string& oid_t, const char *cls, const char *method,
	     bufferlist& inbl, bufferlist& outbl);
    int omap_get_vals(const std::string& oid_t,
		      const std::string& start_after,
		      uint64_t max_return,
		      std::map<std::string, bufferlist> &out_vals);
    int omap_get_vals(const std::string& oid_t,
		      const std::string& start_after,
		      const std::string& filter_prefix,
		      uint64_t max_return,
		      std::map<std::string, bufferlist> &out_vals);
    int omap_get_keys(const std::string& oid_t,
		      const std::string& start_after,
		      uint64_t max_return,
		      std::set<std::string> *out_keys);
    int omap_get_header(const std::string& oid_t,
			bufferlist *bl);
    int omap_get_vals_by_keys(const std::string& oid_t,
			      const std::set<std::string>& keys,
			      std::map<std::string, bufferlist> &vals);
    int omap_set(const std::string& oid_t,
		 const std::map<std::string, bufferlist>& map);
    int omap_set_header(const std::string& oid_t,
			const bufferlist& bl);
    int omap_clear(const std::string& oid_t);
    int omap_rm_keys(const std::string& oid_t,
		     const std::set<std::string>& keys);

    // Advisory locking on rados objects.
    int lock_exclusive(const std::string &oid_t, const std::string &name,
		       const std::string &cookie,
		       const std::string &description,
		       struct timeval * duration, uint8_t flags);

    int lock_shared(const std::string &oid_t, const std::string &name,
		    const std::string &cookie, const std::string &tag,
		    const std::string &description,
		    struct timeval * duration, uint8_t flags);

    int unlock(const std::string &oid_t, const std::string &name,
	       const std::string &cookie);

    int break_lock(const std::string &oid_t, const std::string &name,
		   const std::string &client, const std::string &cookie);

    int list_lockers(const std::string &oid_t, const std::string &name,
		     int *exclusive,
		     std::string *tag,
		     std::list<librados::locker_t> *lockers);

    uint64_t get_last_version();

    int aio_read(const std::string& oid_t, AioCompletion *c,
		 bufferlist *pbl, size_t len, uint64_t off);
    int aio_sparse_read(const std::string& oid_t, AioCompletion *c,
			std::map<uint64_t,uint64_t> *m, bufferlist *data_bl,
			size_t len, uint64_t off);
    int aio_write(const std::string& oid_t, AioCompletion *c, const bufferlist& bl,
		  size_t len, uint64_t off);
    int aio_append(const std::string& oid_t, AioCompletion *c, const bufferlist& bl,
		  size_t len);
    int aio_write_full(const std::string& oid_t, AioCompletion *c, const bufferlist& bl);

    /**
     * Asychronously remove an object
     *
     * Queues the remove and returns.
     *
     * The return value of the completion will be 0 on success, negative
     * error code on failure.
     *
     * @param io the context to operate in
     * @param oid_t the name of the object
     * @param completion what to do when the remove is safe and complete
     * @returns 0 on success
     */
    int aio_remove(const std::string& oid_t, AioCompletion *c);

    /**
     * Wait for all currently pending aio writes to be safe.
     *
     * @returns 0 on success, negative error code on failure
     */
    int aio_flush();

    /**
     * Schedule a callback for when all currently pending
     * aio writes are safe. This is a non-blocking version of
     * aio_flush().
     *
     * @param c what to do when the writes are safe
     * @returns 0 on success, negative error code on failure
     */
    int aio_flush_async(AioCompletion *c);

    int aio_stat(const std::string& oid_t, AioCompletion *c, uint64_t *psize, time_t *pmtime);

    int aio_exec(const std::string& oid_t, AioCompletion *c, const char *cls, const char *method,
		 bufferlist& inbl, bufferlist *outbl);

    // compound object operations
    int operate(const std::string& oid_t, ObjectWriteOperation *op);
    int operate(const std::string& oid_t, ObjectReadOperation *op, bufferlist *pbl);
    int aio_operate(const std::string& oid_t, AioCompletion *c, ObjectWriteOperation *op);
    int aio_operate(const std::string& oid_t, AioCompletion *c, ObjectWriteOperation *op, int flags);
    int aio_operate(const std::string& oid_t, AioCompletion *c,
		    ObjectReadOperation *op, bufferlist *pbl);

    int aio_operate(const std::string& oid_t, AioCompletion *c,
		    ObjectReadOperation *op, int flags,
		    bufferlist *pbl);

    // watch/notify
    int watch(const std::string& o, uint64_t ver, uint64_t *handle,
	      librados::WatchCtx *ctx);
    int unwatch(const std::string& o, uint64_t handle);
    int notify(const std::string& o, bufferlist& bl);
    int list_watchers(const std::string& o, std::list<obj_watch_t> *out_watchers);
    void set_notify_timeout(ceph::timespan timeout);

    /**
     * Set allocation hint for an object
     *
     * This is an advisory operation, it will always succeed (as if it
     * was submitted with a OP_FAILOK flag set) and is not guaranteed
     * to do anything on the backend.
     *
     * @param o the name of the object
     * @param expected_object_size expected size of the object, in bytes
     * @param expected_write_size expected size of writes to the object, in bytes
     * @returns 0 on success, negative error code on failure
     */
    int set_alloc_hint(const std::string& o,
		       uint64_t expected_object_size,
		       uint64_t expected_write_size);

    boost::uuids::uuid get_volume();

    config_t cct();

  private:
    /* You can only get IoCtx instances from Rados */
    IoCtx(IoCtxImpl *io_ctx_impl_);

    friend class Rados; // Only Rados can use our private constructor to create IoCtxes.
    friend class ObjectOperation;

    IoCtxImpl *io_ctx_impl;
  };

  class Rados
  {
  public:
    static void version(int *major, int *minor, int *extra);

    Rados();
    explicit Rados(IoCtx& ioctx);
    ~Rados();

    int init(const char * const id);
    int init2(const char * const name, const char * const clustername,
	      uint64_t flags);
    int init_with_context(config_t cct_);
    config_t cct();
    int connect();
    void shutdown();
    int conf_read_file(const char * const path) const;
    int conf_parse_argv(int argc, const char ** argv) const;
    int conf_parse_argv_remainder(int argc, const char ** argv,
				  const char ** remargv) const;
    int conf_parse_env(const char *env) const;
    int conf_set(const char *option, const char *value);
    int conf_get(const char *option, std::string &val);
    int volume_create(const string &name);
    int volume_delete(const string &name);


    uint64_t get_instance_id();

    int mon_command(std::string cmd, const bufferlist& inbl,
		    bufferlist *outbl, std::string *outs);

    int ioctx_create(const std::string &name, IoCtx &ioctx);

    // Features useful for test cases
    void test_blacklist_self(bool set);

    int cluster_stat(cluster_stat_t& result);
    int cluster_fsid(std::string *fsid);

    /// get/wait for the most recent osdmap
    int wait_for_latest_osdmap();

   // -- aio --
    static AioCompletion *aio_create_completion();
    static AioCompletion *aio_create_completion(void *cb_arg, callback_t cb_complete,
						callback_t cb_safe);

    std::shared_ptr<const Volume> lookup_volume(const string& name);
    std::shared_ptr<const Volume> lookup_volume(const boost::uuids::uuid& name);

    Objecter* objecter();

    friend std::ostream& operator<<(std::ostream &oss, const Rados& r);
    RadosClient *client;
  private:
    // We don't allow assignment or copying
    Rados(const Rados& rhs);
    const Rados& operator=(const Rados& rhs);
  };
}

#endif

