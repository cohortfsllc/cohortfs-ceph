#include <errno.h>

#include "common/Formatter.h"
#include "common/utf8.h"
#include "rgw_common.h"
#include "rgw_access.h"
#include "rgw_formats.h"
#include "rgw_op.h"
#include "rgw_rest.h"
#include "rgw_rest_swift.h"
#include "rgw_rest_s3.h"
#include "rgw_swift_auth.h"

#include "rgw_formats.h"

#define DOUT_SUBSYS rgw

static void dump_status(struct req_state *s, const char *status)
{
  CGI_PRINTF(s,"Status: %s\n", status);
}

struct rgw_html_errors {
  int err_no;
  int http_ret;
  const char *s3_code;
};

const static struct rgw_html_errors RGW_HTML_ERRORS[] = {
    { 0, 200, "" },
    { 201, 201, "Created" },
    { 204, 204, "NoContent" },
    { 206, 206, "" },
    { EINVAL, 400, "InvalidArgument" },
    { ERR_INVALID_DIGEST, 400, "InvalidDigest" },
    { ERR_BAD_DIGEST, 400, "BadDigest" },
    { ERR_INVALID_BUCKET_NAME, 400, "InvalidBucketName" },
    { ERR_INVALID_OBJECT_NAME, 400, "InvalidObjectName" },
    { ERR_UNRESOLVABLE_EMAIL, 400, "UnresolvableGrantByEmailAddress" },
    { ERR_INVALID_PART, 400, "InvalidPart" },
    { ERR_INVALID_PART_ORDER, 400, "InvalidPartOrder" },
    { ERR_REQUEST_TIMEOUT, 400, "RequestTimeout" },
    { ERR_LENGTH_REQUIRED, 411, "MissingContentLength" },
    { EACCES, 403, "AccessDenied" },
    { EPERM, 403, "AccessDenied" },
    { ERR_USER_SUSPENDED, 403, "UserSuspended" },
    { ERR_REQUEST_TIME_SKEWED, 403, "RequestTimeTooSkewed" },
    { ENOENT, 404, "NoSuchKey" },
    { ERR_NO_SUCH_BUCKET, 404, "NoSuchBucket" },
    { ERR_NO_SUCH_UPLOAD, 404, "NoSuchUpload" },
    { ERR_METHOD_NOT_ALLOWED, 405, "MethodNotAllowed" },
    { ETIMEDOUT, 408, "RequestTimeout" },
    { EEXIST, 409, "BucketAlreadyExists" },
    { ENOTEMPTY, 409, "BucketNotEmpty" },
    { ERANGE, 416, "InvalidRange" },
    { ERR_INTERNAL_ERROR, 500, "InternalError" },
};

void set_req_state_err(struct req_state *s, int err_no)
{
  if (err_no < 0)
    err_no = -err_no;
  s->err.ret = err_no;
  for (size_t i = 0; i < sizeof(RGW_HTML_ERRORS)/sizeof(RGW_HTML_ERRORS[0]); ++i) {
    const struct rgw_html_errors *r = RGW_HTML_ERRORS + i;
    if (err_no == r->err_no) {
      s->err.http_ret = r->http_ret;
      s->err.s3_code = r->s3_code;
      return;
    }
  }
  dout(0) << "set_req_state_err err_no=" << err_no << " resorting to 500" << dendl;

  s->err.http_ret = 500;
  s->err.s3_code = "UnknownError";
}

void dump_errno(struct req_state *s)
{
  char buf[32];
  snprintf(buf, sizeof(buf), "%d", s->err.http_ret);
  dump_status(s, buf);
}

void dump_errno(struct req_state *s, int err)
{
  char buf[32];
  snprintf(buf, sizeof(buf), "%d", err);
  dump_status(s, buf);
}

void dump_content_length(struct req_state *s, size_t len)
{
  char buf[16];
  snprintf(buf, sizeof(buf), "%lu", (long unsigned int)len);
  CGI_PRINTF(s, "Content-Length: %s\n", buf);
  CGI_PRINTF(s, "Accept-Ranges: %s\n", "bytes");
}

void dump_etag(struct req_state *s, const char *etag)
{
  if (s->prot_flags & RGW_REST_SWIFT)
    CGI_PRINTF(s,"etag: %s\n", etag);
  else
    CGI_PRINTF(s,"ETag: \"%s\"\n", etag);
}

void dump_last_modified(struct req_state *s, time_t t)
{

  char timestr[TIME_BUF_SIZE];
  struct tm *tmp = gmtime(&t);
  if (tmp == NULL)
    return;

  if (strftime(timestr, sizeof(timestr), "%a, %d %b %Y %H:%M:%S %Z", tmp) == 0)
    return;

  CGI_PRINTF(s, "Last-Modified: %s\n", timestr);
}

void dump_time(struct req_state *s, const char *name, time_t *t)
{
  char buf[TIME_BUF_SIZE];
  struct tm result;
  struct tm *tmp = gmtime_r(t, &result);
  if (tmp == NULL)
    return;

  if (strftime(buf, sizeof(buf), "%Y-%m-%dT%T.000Z", tmp) == 0)
    return;

  s->formatter->dump_format(name, buf); 
}

void dump_owner(struct req_state *s, string& id, string& name, const char *section)
{
  if (!section)
    section = "Owner";
  s->formatter->open_object_section(section);
  s->formatter->dump_format("ID", id.c_str());
  s->formatter->dump_format("DisplayName", name.c_str());
  s->formatter->close_section();
}

void dump_start(struct req_state *s)
{
  if (!s->content_started) {
    if (s->format == RGW_FORMAT_XML)
      s->formatter->write_raw_data(XMLFormatter::XML_1_DTD);
    s->content_started = true;
  }
}

void end_header(struct req_state *s, const char *content_type)
{
  if (!content_type || s->err.is_err()) {
    switch (s->format) {
    case RGW_FORMAT_XML:
      content_type = "application/xml";
      break;
    case RGW_FORMAT_JSON:
      content_type = "application/json";
      break;
    default:
      content_type = "text/plain";
      break;
    }
  }
  if (s->err.is_err()) {
    dump_start(s);
    s->formatter->open_object_section("Error");
    if (!s->err.s3_code.empty())
      s->formatter->dump_format("Code", "%s", s->err.s3_code.c_str());
    if (!s->err.message.empty())
      s->formatter->dump_format("Message", s->err.message.c_str());
    s->formatter->close_section();
    dump_content_length(s, s->formatter->get_len());
  }
  CGI_PRINTF(s,"Content-type: %s\r\n\r\n", content_type);
  flush_formatter_to_req_state(s, s->formatter);
  s->header_ended = true;
}

void abort_early(struct req_state *s, int err_no)
{
  set_req_state_err(s, err_no);
  dump_errno(s);
  end_header(s);
  flush_formatter_to_req_state(s, s->formatter);
}

void dump_continue(struct req_state *s)
{
  dump_status(s, "100");
  FCGX_FFlush(s->fcgx->out);
}

void dump_range(struct req_state *s, off_t ofs, off_t end, size_t total)
{
    CGI_PRINTF(s,"Content-Range: bytes %d-%d/%d\n", (int)ofs, (int)end, (int)total);
}

int RGWGetObj_REST::get_params()
{
  range_str = s->env->get("HTTP_RANGE");
  if_mod = s->env->get("HTTP_IF_MODIFIED_SINCE");
  if_unmod = s->env->get("HTTP_IF_UNMODIFIED_SINCE");
  if_match = s->env->get("HTTP_IF_MATCH");
  if_nomatch = s->env->get("HTTP_IF_NONE_MATCH");

  return 0;
}


int RGWPutObj_REST::get_params()
{
  supplied_md5_b64 = s->env->get("HTTP_CONTENT_MD5");

  return 0;
}

int RGWPutObj_REST::get_data()
{
  size_t cl;
  if (s->length) {
    cl = atoll(s->length) - ofs;
    if (cl > RGW_MAX_CHUNK_SIZE)
      cl = RGW_MAX_CHUNK_SIZE;
  } else {
    cl = RGW_MAX_CHUNK_SIZE;
  }

  int len = 0;
  if (cl) {
    data = (char *)malloc(cl);
    if (!data)
       return -ENOMEM;

    CGI_GetStr(s, data, cl, len);
  }

  if (!ofs)
    supplied_md5_b64 = s->env->get("HTTP_CONTENT_MD5");

  return len;
}

int RGWCopyObj_REST::get_params()
{
  if_mod = s->env->get("HTTP_X_AMZ_COPY_IF_MODIFIED_SINCE");
  if_unmod = s->env->get("HTTP_X_AMZ_COPY_IF_UNMODIFIED_SINCE");
  if_match = s->env->get("HTTP_X_AMZ_COPY_IF_MATCH");
  if_nomatch = s->env->get("HTTP_X_AMZ_COPY_IF_NONE_MATCH");

  return 0;
}

int RGWPutACLs_REST::get_params()
{
  size_t cl = 0;
  if (s->length)
    cl = atoll(s->length);
  if (cl) {
    data = (char *)malloc(cl + 1);
    if (!data) {
       ret = -ENOMEM;
       return ret;
    }
    CGI_GetStr(s, data, cl, len);
    data[len] = '\0';
  } else {
    len = 0;
  }

  return ret;
}

int RGWInitMultipart_REST::get_params()
{
  if (!s->args.exists("uploads")) {
    ret = -ENOTSUP;
  }

  return ret;
}

int RGWCompleteMultipart_REST::get_params()
{
  upload_id = s->args.get("uploadId");

  if (upload_id.empty()) {
    ret = -ENOTSUP;
    return ret;
  }

  size_t cl = 0;

  if (s->length)
    cl = atoll(s->length);
  if (cl) {
    data = (char *)malloc(cl + 1);
    if (!data) {
       ret = -ENOMEM;
       return ret;
    }
    CGI_GetStr(s, data, cl, len);
    data[len] = '\0';
  } else {
    len = 0;
  }

  return ret;
}

int RGWListMultipart_REST::get_params()
{
  upload_id = s->args.get("uploadId");

  if (upload_id.empty()) {
    ret = -ENOTSUP;
  }
  string str = s->args.get("part-number-marker");
  if (!str.empty())
    marker = atoi(str.c_str());
  
  str = s->args.get("max-parts");
  if (!str.empty())
    max_parts = atoi(str.c_str());

  return ret;
}

int RGWListBucketMultiparts_REST::get_params()
{
  url_decode(s->args.get("delimiter"), delimiter);
  url_decode(s->args.get("prefix"), prefix);
  string str = s->args.get("max-parts");
  if (!str.empty())
    max_uploads = atoi(str.c_str());
  else
    max_uploads = default_max;

  string key_marker = s->args.get("key-marker");
  string upload_id_marker = s->args.get("upload-id-marker");
  if (!key_marker.empty())
    marker.init(key_marker, upload_id_marker);

  return 0;
}

static void next_tok(string& str, string& tok, char delim)
{
  if (str.size() == 0) {
    tok = "";
    return;
  }
  tok = str;
  int pos = str.find(delim);
  if (pos > 0) {
    tok = str.substr(0, pos);
    str = str.substr(pos + 1);
  } else {
    str = "";
  }
}

void init_entities_from_header(struct req_state *s)
{
  string req;
  string first;

  s->bucket_name = NULL;
  s->bucket.clear();
  s->object = NULL;
  s->object_str = "";

  s->header_ended = false;
  s->bytes_sent = 0;
  s->bytes_received = 0;
  s->obj_size = 0;

  /* this is the default, might change in a few lines */
  s->format = RGW_FORMAT_XML;
  s->formatter = new XMLFormatter(false);

  int pos;
  if (g_conf->rgw_dns_name.length() && s->host) {
    string h(s->host);

    dout(10) << "host=" << s->host << " rgw_dns_name=" << g_conf->rgw_dns_name << dendl;
    pos = h.find(g_conf->rgw_dns_name);

    if (pos > 0 && h[pos - 1] == '.') {
      string encoded_bucket = h.substr(0, pos-1);
      url_decode(encoded_bucket, s->bucket_name_str);
      s->bucket_name = strdup(s->bucket_name_str.c_str());
      s->host_bucket = s->bucket_name;
    } else {
      s->host_bucket = NULL;
    }
  } else s->host_bucket = NULL;

  const char *req_name = s->path_name;
  const char *p;

  if (*req_name == '?') {
    p = req_name;
  } else {
    p = s->query;
  }

  s->args.set(p);
  s->args.parse();

  if (*req_name != '/')
    goto done;

  req_name++;

  if (!*req_name)
    goto done;

  req = req_name;

  pos = req.find('/');
  if (pos >= 0) {
    bool cut_url = g_conf->rgw_swift_url_prefix.length();
    first = req.substr(0, pos);
    if (first.compare(g_conf->rgw_swift_url_prefix) == 0) {
      s->prot_flags |= RGW_REST_SWIFT;
      if (cut_url) {
        next_tok(req, first, '/');
      }
    }
  } else {
    first = req;
  }

  if (s->prot_flags & RGW_REST_SWIFT) {
    s->format = 0;
    delete s->formatter;
    s->formatter = new RGWFormatter_Plain;
    string format_str = s->args.get("format");
    if (format_str.compare("xml") == 0) {
      s->format = RGW_FORMAT_XML;
      delete s->formatter;
      s->formatter = new XMLFormatter(false);
    } else if (format_str.compare("json") == 0) {
      s->format = RGW_FORMAT_JSON;
      delete s->formatter;
      s->formatter = new JSONFormatter(false);
    }
  }

  if (s->prot_flags & RGW_REST_SWIFT) {
    string ver;

    next_tok(req, ver, '/');
    dout(10) << "ver=" << ver << dendl;
    s->os_auth_token = s->env->get("HTTP_X_AUTH_TOKEN");
    next_tok(req, first, '/');

    dout(10) << "ver=" << ver << " first=" << first << " req=" << req << dendl;
    if (first.size() == 0)
      goto done;

    url_decode(first, s->bucket_name_str);
    s->bucket_name = strdup(s->bucket_name_str.c_str());
   
    if (req.size()) {
      url_decode(req, s->object_str);
      s->object = strdup(s->object_str.c_str());
    }

    goto done;
  }

  if (!s->bucket_name) {
    url_decode(first, s->bucket_name_str);
    s->bucket_name = strdup(s->bucket_name_str.c_str());
  } else {
    url_decode(req, s->object_str);
    s->object = strdup(s->object_str.c_str());
    goto done;
  }

  if (strcmp(s->bucket_name, "auth") == 0)
    s->prot_flags |= RGW_REST_SWIFT_AUTH;

  if (pos >= 0) {
    string encoded_obj_str = req.substr(pos+1);
    url_decode(encoded_obj_str, s->object_str);

    if (s->object_str.size() > 0) {
      s->object = strdup(s->object_str.c_str());
    }
  }
done:
  s->formatter->reset();
}

static void line_unfold(const char *line, string& sdest)
{
  char dest[strlen(line) + 1];
  const char *p = line;
  char *d = dest;

  while (isspace(*p))
    ++p;

  bool last_space = false;

  while (*p) {
    switch (*p) {
    case '\n':
    case '\r':
      *d = ' ';
      if (!last_space)
        ++d;
      last_space = true;
      break;
    default:
      *d = *p;
      ++d;
      last_space = false;
      break;
    }
    ++p;
  }
  *d = 0;
  sdest = dest;
}

struct str_len {
  const char *str;
  int len;
};

#define STR_LEN_ENTRY(s) { s, sizeof(s) - 1 }

struct str_len meta_prefixes[] = { STR_LEN_ENTRY("HTTP_X_AMZ"),
                                   STR_LEN_ENTRY("HTTP_X_GOOG"),
                                   STR_LEN_ENTRY("HTTP_X_DHO"),
                                   {NULL, 0} };

static void init_auth_info(struct req_state *s)
{
  const char *p;

  s->x_meta_map.clear();

  for (int i=0; (p = s->fcgx->envp[i]); ++i) {
    const char *prefix;
    for (int prefix_num = 0; (prefix = meta_prefixes[prefix_num].str) != NULL; prefix_num++) {
      int len = meta_prefixes[prefix_num].len;
      if (strncmp(p, prefix, len) == 0) {
        dout(10) << "meta>> " << p << dendl;
        const char *name = p+5; /* skip the HTTP_ part */
        const char *eq = strchr(name, '=');
        if (!eq) /* shouldn't happen! */
          continue;
        int len = eq - name;
        char name_low[len + 1];
        int j;
        for (j=0; j<len; j++) {
          name_low[j] = tolower(name[j]);
          if (name_low[j] == '_')
            name_low[j] = '-';
        }
        name_low[j] = 0;
        string val;
        line_unfold(eq + 1, val);

        map<string, string>::iterator iter;
        iter = s->x_meta_map.find(name_low);
        if (iter != s->x_meta_map.end()) {
          string old = iter->second;
          int pos = old.find_last_not_of(" \t"); /* get rid of any whitespaces after the value */
          old = old.substr(0, pos + 1);
          old.append(",");
          old.append(val);
          s->x_meta_map[name_low] = old;
        } else {
          s->x_meta_map[name_low] = val;
        }
      }
    }
  }
  map<string, string>::iterator iter;
  for (iter = s->x_meta_map.begin(); iter != s->x_meta_map.end(); ++iter) {
    dout(10) << "x>> " << iter->first << ":" << iter->second << dendl;
  }

}

static bool looks_like_ip_address(const char *bucket)
{
  int num_periods = 0;
  bool expect_period = false;
  for (const char *b = bucket; *b; ++b) {
    if (*b == '.') {
      if (!expect_period)
	return false;
      ++num_periods;
      if (num_periods > 3)
	return false;
      expect_period = false;
    }
    else if (isdigit(*b)) {
      expect_period = true;
    }
    else {
      return false;
    }
  }
  return (num_periods == 3);
}

// This function enforces Amazon's spec for bucket names.
// (The requirements, not the recommendations.)
static int validate_bucket_name(const char *bucket)
{
  int len = strlen(bucket);
  if (len < 3) {
    if (len == 0) {
      // This request doesn't specify a bucket at all
      return 0;
    }
    // Name too short
    return ERR_INVALID_BUCKET_NAME;
  }
  else if (len > 255) {
    // Name too long
    return ERR_INVALID_BUCKET_NAME;
  }

  if (!(isalpha(bucket[0]) || isdigit(bucket[0]))) {
    // bucket names must start with a number or letter
    return ERR_INVALID_BUCKET_NAME;
  }

  for (const char *s = bucket; *s; ++s) {
    char c = *s;
    if (isdigit(c) || (c == '.'))
      continue;
    if (isalpha(c))
      continue;
    if ((c == '-') || (c == '_'))
      continue;
    // Invalid character
    return ERR_INVALID_BUCKET_NAME;
  }

  if (looks_like_ip_address(bucket))
    return ERR_INVALID_BUCKET_NAME;
  return 0;
}

// "The name for a key is a sequence of Unicode characters whose UTF-8 encoding
// is at most 1024 bytes long."
// However, we can still have control characters and other nasties in there.
// Just as long as they're utf-8 nasties.
static int validate_object_name(const char *object)
{
  int len = strlen(object);
  if (len > 1024) {
    // Name too long
    return ERR_INVALID_OBJECT_NAME;
  }

  if (check_utf8(object, len)) {
    // Object names must be valid UTF-8.
    return ERR_INVALID_OBJECT_NAME;
  }
  return 0;
}

int RGWHandler_REST::preprocess(struct req_state *s, FCGX_Request *fcgx)
{
  int ret = 0;

  s->fcgx = fcgx;
  s->path_name = s->env->get("SCRIPT_NAME");
  s->path_name_url = s->env->get("REQUEST_URI");
  int pos = s->path_name_url.find('?');
  if (pos >= 0) {
    s->path_name_url = s->path_name_url.substr(0, pos);
  }
  s->method = s->env->get("REQUEST_METHOD");
  s->host = s->env->get("HTTP_HOST");
  s->query = s->env->get("QUERY_STRING");
  s->length = s->env->get("CONTENT_LENGTH");
  s->content_type = s->env->get("CONTENT_TYPE");
  s->prot_flags = 0;

  if (!s->method)
    s->op = OP_UNKNOWN;
  else if (strcmp(s->method, "GET") == 0)
    s->op = OP_GET;
  else if (strcmp(s->method, "PUT") == 0)
    s->op = OP_PUT;
  else if (strcmp(s->method, "DELETE") == 0)
    s->op = OP_DELETE;
  else if (strcmp(s->method, "HEAD") == 0)
    s->op = OP_HEAD;
  else if (strcmp(s->method, "POST") == 0)
    s->op = OP_POST;
  else
    s->op = OP_UNKNOWN;

  init_entities_from_header(s);
  switch (s->op) {
  case OP_PUT:
    if (s->object && !s->args.sub_resource_exists("acl")) {
      if (!s->length)
        ret = -ERR_LENGTH_REQUIRED;
      else if (*s->length == '\0')
        ret = -EINVAL;
    }
    if (s->length)
      s->content_length = atoll(s->length);
    else
      s->content_length = 0;
    break;
  default:
    break;
  }

  if (ret)
    return ret;

  ret = validate_bucket_name(s->bucket_name_str.c_str());
  if (ret)
    return ret;
  ret = validate_object_name(s->object_str.c_str());
  if (ret)
    return ret;
  dout(10) << "s->object=" << (s->object ? s->object : "<NULL>") << " s->bucket=" << (s->bucket_name ? s->bucket_name : "<NULL>") << dendl;

  init_auth_info(s);

  const char *cacl = s->env->get("HTTP_X_AMZ_ACL");
  if (cacl)
    s->canned_acl = cacl;

  s->copy_source = s->env->get("HTTP_X_AMZ_COPY_SOURCE");
  s->http_auth = s->env->get("HTTP_AUTHORIZATION");

  if (g_conf->rgw_print_continue) {
    const char *expect = s->env->get("HTTP_EXPECT");
    s->expect_cont = (expect && !strcasecmp(expect, "100-continue"));
  }
  return ret;
}

int RGWHandler_REST::read_permissions()
{
  bool only_bucket;

  switch (s->op) {
  case OP_HEAD:
  case OP_GET:
    only_bucket = false;
    break;
  case OP_PUT:
  case OP_POST:
    if (is_acl_op()) {
      only_bucket = false;
      break;
    }
    /* is it a 'create bucket' request? */
    if (s->object_str.size() == 0)
      return 0;
  case OP_DELETE:
    only_bucket = true;
    break;
  default:
    return -EINVAL;
  }

  return do_read_permissions(only_bucket);
}

RGWOp *RGWHandler_REST::get_op()
{
  RGWOp *op;
  switch (s->op) {
   case OP_GET:
     op = get_retrieve_op(true);
     break;
   case OP_PUT:
     op = get_create_op();
     break;
   case OP_DELETE:
     op = get_delete_op();
     break;
   case OP_HEAD:
     op = get_retrieve_op(false);
     break;
   case OP_POST:
     op = get_post_op();
     break;
   default:
     return NULL;
  }

  if (op) {
    op->init(s);
  }
  return op;
}


RGWRESTMgr::RGWRESTMgr()
{
  m_os_handler = new RGWHandler_REST_SWIFT;
  m_os_auth_handler = new RGWHandler_SWIFT_Auth;
  m_s3_handler = new RGWHandler_REST_S3;
}

RGWRESTMgr::~RGWRESTMgr()
{
  delete m_os_handler;
  delete m_os_auth_handler;
  delete m_s3_handler;
}

RGWHandler *RGWRESTMgr::get_handler(struct req_state *s, FCGX_Request *fcgx,
				    int *init_error)
{
  RGWHandler *handler;

  *init_error = RGWHandler_REST::preprocess(s, fcgx);

  if (s->prot_flags & RGW_REST_SWIFT)
    handler = m_os_handler;
  else if (s->prot_flags & RGW_REST_SWIFT_AUTH)
    handler = m_os_auth_handler;
  else
    handler = m_s3_handler;

  handler->init(s, fcgx);

  return handler;
}

void RGWHandler_REST::put_op(RGWOp *op)
{
  delete op;
}

