#ifndef CEPH_RGW_LOADGEN_H
#define CEPH_RGW_LOADGEN_H

#include "rgw_client_io.h"


struct RGWLoadGenRequestEnv {
  int port;
  uint64_t content_length;
  string content_type;
  string request_method;
  string uri;
  string query_string;
  string date_str;

  map<string, string> headers;

  RGWLoadGenRequestEnv() : port(0), content_length(0) {}

  void set_date(const ceph::real_time& tm);
  int sign(RGWAccessKey& access_key);
};

class RGWLoadGenIO : public RGWClientIO
{
  uint64_t left_to_read;
  RGWLoadGenRequestEnv *req;
public:
  void init_env();

  int write_data(const char *buf, int len);
  int read_data(char *buf, int len);

  int send_status(const char *status, const char *status_name);
  int send_100_continue();
  int complete_header();
  int complete_request();
  int send_content_length(uint64_t len);

RGWLoadGenIO(CephContext* cct, RGWLoadGenRequestEnv *_re)
	: RGWClientIO(cct), left_to_read(0), req(_re) {}
  void flush();
};


#endif
