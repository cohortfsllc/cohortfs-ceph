// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include "include/ceph_time.h"
#include "common/Timer.h"
#include "common/utf8.h"
#include "common/OutputDataSocket.h"
#include "common/Formatter.h"

#include "rgw_log.h"
#include "rgw_acl.h"
#include "rgw_rados.h"
#include "rgw_client_io.h"

#define dout_subsys ceph_subsys_rgw

static void set_param_str(struct req_state *s, const char *name, string& str)
{
  const char *p = s->info.env->get(name);
  if (p)
    str = p;
}

string render_log_object_name(const string& format,
			      struct tm *dt, string& bucket_id, const string& bucket_name)
{
  string o;
  for (unsigned i=0; i<format.size(); i++) {
    if (format[i] == '%' && i+1 < format.size()) {
      i++;
      char buf[32];
      switch (format[i]) {
      case '%':
	strcpy(buf, "%");
	break;
      case 'Y':
	sprintf(buf, "%.4d", dt->tm_year + 1900);
	break;
      case 'y':
	sprintf(buf, "%.2d", dt->tm_year % 100);
	break;
      case 'm':
	sprintf(buf, "%.2d", dt->tm_mon + 1);
	break;
      case 'd':
	sprintf(buf, "%.2d", dt->tm_mday);
	break;
      case 'H':
	sprintf(buf, "%.2d", dt->tm_hour);
	break;
      case 'I':
	sprintf(buf, "%.2d", (dt->tm_hour % 12) + 1);
	break;
      case 'k':
	sprintf(buf, "%d", dt->tm_hour);
	break;
      case 'l':
	sprintf(buf, "%d", (dt->tm_hour % 12) + 1);
	break;
      case 'M':
	sprintf(buf, "%.2d", dt->tm_min);
	break;

      case 'i':
	o += bucket_id;
	continue;
      case 'n':
	o += bucket_name;
	continue;
      default:
	// unknown code
	sprintf(buf, "%%%c", format[i]);
	break;
      }
      o += buf;
      continue;
    }
    o += format[i];
  }
  return o;
}

/* usage logger */
class UsageLogger {
  CephContext *cct;
  RGWRados *store;
  map<rgw_user_bucket, RGWUsageBatch> usage_map;
  std::mutex lock;
  typedef std::lock_guard<std::mutex> lock_guard;
  typedef std::unique_lock<std::mutex> unique_lock;
  int32_t num_entries;
  cohort::Timer<ceph::mono_clock> timer;
  ceph::real_time round_timestamp;

  class UsageLogTimeout {
    UsageLogger& logger;
  public:
    UsageLogTimeout(UsageLogger& _l) : logger(_l) {}
    void operator()() {
      logger.flush();
      logger.timer.reschedule_me(
	logger.cct->_conf->rgw_usage_log_tick_interval);
    }
  };

  void set_timer() {
    timer.add_event(cct->_conf->rgw_usage_log_tick_interval,
		    UsageLogTimeout(*this));
  }
public:

  UsageLogger(CephContext *_cct, RGWRados *_store)
      : cct(_cct), store(_store), num_entries(0) {
    set_timer();
    ceph::real_time ts = ceph::real_clock::now();
    recalc_round_timestamp(ts);
  }

  ~UsageLogger() {
    flush();
    timer.cancel_all_events();
  }

  void recalc_round_timestamp(ceph::real_time ts) {
    round_timestamp = std::chrono::time_point_cast<std::chrono::hours>(ts);
  }

  void insert(const ceph::real_time& timestamp, rgw_usage_log_entry& entry) {
    unique_lock l(lock);
    if (timestamp > (round_timestamp + 1h))
      recalc_round_timestamp(timestamp);
    bool account;
    rgw_user_bucket ub(entry.owner, entry.bucket);
    usage_map[ub].insert(round_timestamp, entry, &account);
    if (account)
      num_entries++;
    bool need_flush = (num_entries > cct->_conf->rgw_usage_log_flush_threshold);
    l.unlock();
    if (need_flush) {
      flush();
    }
  }

  void flush() {
    map<rgw_user_bucket, RGWUsageBatch> old_map;
    unique_lock l(lock);
    old_map.swap(usage_map);
    num_entries = 0;
    l.unlock();

    store->log_usage(old_map);
  }
};

static UsageLogger *usage_logger = NULL;

void rgw_log_usage_init(CephContext *cct, RGWRados *store)
{
  usage_logger = new UsageLogger(cct, store);
}

void rgw_log_usage_finalize()
{
  delete usage_logger;
  usage_logger = NULL;
}

static void log_usage(struct req_state *s, const string& op_name)
{
  if (s->system_request) /* don't log system user operations */
    return;

  if (!usage_logger)
    return;

  string user;

  if (!s->bucket_name_str.empty())
    user = s->bucket_owner.get_id();
  else
    user = s->user.user_id;

  rgw_usage_log_entry entry(user, s->bucket.name);

  uint64_t bytes_sent = s->cio->get_bytes_sent();
  uint64_t bytes_received = s->cio->get_bytes_received();

  rgw_usage_data data(bytes_sent, bytes_received);

  data.ops = 1;
  if (!s->err.is_err())
    data.successful_ops = 1;

  entry.add(op_name, data);

  auto ts = ceph::real_clock::now();

  usage_logger->insert(ts, entry);
}

void rgw_format_ops_log_entry(struct rgw_log_entry& entry,
			      Formatter *formatter)
{
  formatter->open_object_section("log_entry");
  formatter->dump_string("bucket", entry.bucket);
  formatter->dump_stream("time_local") << entry.time;
  formatter->dump_string("remote_addr", entry.remote_addr);
  if (entry.object_owner.length())
    formatter->dump_string("object_owner", entry.object_owner);
  formatter->dump_string("user", entry.user);
  formatter->dump_string("operation", entry.op);
  formatter->dump_string("uri", entry.uri);
  formatter->dump_string("http_status", entry.http_status);
  formatter->dump_string("error_code", entry.error_code);
  formatter->dump_int("bytes_sent", entry.bytes_sent);
  formatter->dump_int("bytes_received", entry.bytes_received);
  formatter->dump_int("object_size", entry.obj_size);

  formatter->dump_stream("total_time") << entry.total_time;
  formatter->dump_string("user_agent",	entry.user_agent);
  formatter->dump_string("referrer",  entry.referrer);
  formatter->close_section();
}

void OpsLogSocket::formatter_to_bl(bufferlist& bl)
{
  stringstream ss;
  formatter->flush(ss);
  const string& s = ss.str();

  bl.append(s);
}

void OpsLogSocket::init_connection(bufferlist& bl)
{
  bl.append("[");
}

OpsLogSocket::OpsLogSocket(CephContext *cct, uint64_t _backlog) : OutputDataSocket(cct, _backlog)
{
  formatter = new JSONFormatter;
  delim.append(",\n");
}

OpsLogSocket::~OpsLogSocket()
{
  delete formatter;
}

void OpsLogSocket::log(struct rgw_log_entry& entry)
{
  bufferlist bl;

  unique_lock l(lock);
  rgw_format_ops_log_entry(entry, formatter);
  formatter_to_bl(bl);
  l.unlock();

  append_output(bl);
}

int rgw_log_op(RGWRados *store, struct req_state *s, const string& op_name, OpsLogSocket *olog)
{
  struct rgw_log_entry entry;
  string bucket_id;

  if (s->enable_usage_log)
    log_usage(s, op_name);

  if (!s->enable_ops_log)
    return 0;

  if (s->bucket_name_str.empty()) {
    ldout(s->cct, 5) << "nothing to log for operation" << dendl;
    return -EINVAL;
  }
  if (s->err.ret == -ERR_NO_SUCH_BUCKET) {
    if (!s->cct->_conf->rgw_log_nonexistent_bucket) {
      ldout(s->cct, 5) << "bucket " << s->bucket << " doesn't exist, not logging" << dendl;
      return 0;
    }
    bucket_id = "";
  } else {
    bucket_id = s->bucket.bucket_id;
  }
  entry.bucket = s->bucket_name_str;

  if (check_utf8(s->bucket_name_str.c_str(), entry.bucket.size()) != 0) {
    ldout(s->cct, 5) << "not logging op on bucket with non-utf8 name" << dendl;
    return 0;
  }

  if (s->object)
    entry.oid = s->object;
  else
    entry.oid = "-";

  entry.obj_size = s->obj_size;

  if (s->cct->_conf->rgw_remote_addr_param.length())
    set_param_str(s, s->cct->_conf->rgw_remote_addr_param.c_str(), entry.remote_addr);
  else
    set_param_str(s, "REMOTE_ADDR", entry.remote_addr);
  set_param_str(s, "HTTP_USER_AGENT", entry.user_agent);
  set_param_str(s, "HTTP_REFERRER", entry.referrer);
  set_param_str(s, "REQUEST_URI", entry.uri);
  set_param_str(s, "REQUEST_METHOD", entry.op);

  entry.user = s->user.user_id;
  if (s->object_acl)
    entry.object_owner = s->object_acl->get_owner().get_id();
  entry.bucket_owner = s->bucket_owner.get_id();


  uint64_t bytes_sent = s->cio->get_bytes_sent();
  uint64_t bytes_received = s->cio->get_bytes_received();

  entry.time = s->time;
  entry.total_time = ceph::real_clock::now() - s->time;
  entry.bytes_sent = bytes_sent;
  entry.bytes_received = bytes_received;
  if (s->err.http_ret) {
    char buf[16];
    snprintf(buf, sizeof(buf), "%d", s->err.http_ret);
    entry.http_status = buf;
  } else
    entry.http_status = "200"; // default

  entry.error_code = s->err.s3_code;
  entry.bucket_id = bucket_id;

  bufferlist bl;
  ::encode(entry, bl);

  struct tm bdt;
  time_t t = ceph::real_clock::to_time_t(entry.time);
  if (s->cct->_conf->rgw_log_object_name_utc)
    gmtime_r(&t, &bdt);
  else
    localtime_r(&t, &bdt);

  int ret = 0;

  if (s->cct->_conf->rgw_ops_log_rados) {
    string oid = render_log_object_name(s->cct->_conf->rgw_log_object_name, &bdt,
					s->bucket.bucket_id, entry.bucket);

    rgw_obj obj(store->zone.log_vol, oid);

    ret = store->append_async(obj, bl.length(), bl);
    if (ret == -ENOENT) {
      ret = store->create_vol(store->zone.log_vol);
      if (ret < 0)
	goto done;
      // retry
      ret = store->append_async(obj, bl.length(), bl);
    }
  }

  if (olog) {
    olog->log(entry);
  }
done:
  if (ret < 0)
    ldout(s->cct, 0) << "ERROR: failed to log entry" << dendl;

  return ret;
}

int rgw_log_intent(RGWRados *store, rgw_obj& oid, RGWIntentEvent intent,
		   const ceph::real_time& timestamp, bool utc)
{
  rgw_bucket intent_log_bucket(store->zone.intent_log_vol);

  rgw_intent_log_entry entry;
  entry.oid = oid;
  entry.intent = (uint32_t)intent;
  entry.op_time = timestamp;

  struct tm bdt;
  time_t t = ceph::real_clock::to_time_t(timestamp);
  if (utc)
    gmtime_r(&t, &bdt);
  else
    localtime_r(&t, &bdt);

  struct rgw_bucket& bucket = oid.bucket;

  char buf[bucket.name.size() + bucket.bucket_id.size() + 16];
  sprintf(buf, "%.4d-%.2d-%.2d-%s-%s", (bdt.tm_year+1900), (bdt.tm_mon+1), bdt.tm_mday,
	  bucket.bucket_id.c_str(), oid.bucket.name.c_str());
  string oid_t(buf);
  rgw_obj log_obj(intent_log_bucket, oid_t);

  bufferlist bl;
  ::encode(entry, bl);

  int ret = store->append_async(log_obj, bl.length(), bl);
  if (ret == -ENOENT) {
    ret = store->create_vol(intent_log_bucket);
    if (ret < 0)
      goto done;
    ret = store->append_async(log_obj, bl.length(), bl);
  }

done:
  return ret;
}

int rgw_log_intent(RGWRados *store, struct req_state *s, rgw_obj& oid, RGWIntentEvent intent)
{
  return rgw_log_intent(store, oid, intent, s->time, s->cct->_conf->rgw_intent_log_object_name_utc);
}
