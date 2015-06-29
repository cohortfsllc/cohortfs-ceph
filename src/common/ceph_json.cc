// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include <iostream>
#include <include/types.h>
#include <limits.h>
#include <errno.h>
#include "common/strtol.h"

#include "common/ceph_json.h"

// for testing DELETE ME
#include <fstream>

using namespace std;
using namespace json_spirit;

#define dout_subsys ceph_subsys_rgw

void JSONObjIter::set(const JSONObjIter::map_iter_t &_cur, const JSONObjIter::map_iter_t &_last)
{
  cur = _cur;
  last = _last;
}

void JSONObjIter::operator++()
{
  if (cur != last)
    ++cur;
};

JSONObj *JSONObjIter::operator*()
{
  return cur->second;
};

// does not work, FIXME
ostream& operator<<(ostream& out, JSONObj& oid) {
   out << oid.name << ": " << oid.data_string;
   return out;
}

JSONObj::~JSONObj()
{
  multimap<string, JSONObj *>::iterator iter;
  for (iter = children.begin(); iter != children.end(); ++iter) {
    JSONObj *oid = iter->second;
    delete oid;
  }
}


void JSONObj::add_child(string el, JSONObj *oid)
{
  children.insert(pair<string, JSONObj *>(el, oid));
}

bool JSONObj::get_attr(string name, string& attr)
{
  map<string, string>::iterator iter = attr_map.find(name);
  if (iter == attr_map.end())
    return false;
  attr = iter->second;
  return true;
}

JSONObjIter JSONObj::find(const string& name)
{
  JSONObjIter iter;
  map<string, JSONObj *>::iterator first;
  map<string, JSONObj *>::iterator last;
  first = children.find(name);
  if (first != children.end()) {
    last = children.upper_bound(name);
    iter.set(first, last);
  }
  return iter;
}

JSONObjIter JSONObj::find_first()
{
  JSONObjIter iter;
  iter.set(children.begin(), children.end());
  return iter;
}

JSONObjIter JSONObj::find_first(const string& name)
{
  JSONObjIter iter;
  map<string, JSONObj *>::iterator first;
  first = children.find(name);
  iter.set(first, children.end());
  return iter;
}

JSONObj *JSONObj::find_obj(const string& name)
{
  JSONObjIter iter = find(name);
  if (iter.end())
    return NULL;

  return *iter;
}

bool JSONObj::get_data(const string& key, string *dest)
{
  JSONObj *oid = find_obj(key);
  if (!oid)
    return false;

  *dest = oid->get_data();

  return true;
}

/* accepts a JSON Array or JSON Object contained in
 * a JSON Spirit Value, v,  and creates a JSONObj for each
 * child contained in v
 */
void JSONObj::handle_value(Value v)
{
  if (v.type() == obj_type) {
    Object temp_obj = v.get_obj();
    for (Object::size_type i = 0; i < temp_obj.size(); i++) {
      Pair temp_pair = temp_obj[i];
      string temp_name = temp_pair.name_;
      Value temp_value = temp_pair.value_;
      JSONObj *child = new JSONObj;
      child->init(this, temp_value, temp_name);
      add_child(temp_name, child);
    }
  } else if (v.type() == array_type) {
    Array temp_array = v.get_array();
    Value value;

    for (unsigned j = 0; j < temp_array.size(); j++) {
      Value cur = temp_array[j];
      string temp_name;

      JSONObj *child = new JSONObj;
      child->init(this, cur, temp_name);
      add_child(child->get_name(), child);
    }
  }
}

void JSONObj::init(JSONObj *p, Value v, string n)
{
  name = n;
  parent = p;
  data = v;

  handle_value(v);
  if (v.type() == str_type)
    data_string =  v.get_str();
  else
    data_string =  write(v, raw_utf8);
  attr_map.insert(pair<string,string>(name, data_string));
}

JSONObj *JSONObj::get_parent()
{
  return parent;
}

bool JSONObj::is_object()
{
  return (data.type() == obj_type);
}

bool JSONObj::is_array()
{
  return (data.type() == array_type);
}

vector<string> JSONObj::get_array_elements()
{
  vector<string> elements;
  Array temp_array;

  if (data.type() == array_type)
    temp_array = data.get_array();

  int array_size = temp_array.size();
  if (array_size > 0)
    for (int i = 0; i < array_size; i++) {
      Value temp_value = temp_array[i];
      string temp_string;
      temp_string = write(temp_value, raw_utf8);
      elements.push_back(temp_string);
    }

  return elements;
}

JSONParser::JSONParser() : buf_len(0), success(true)
{
}

JSONParser::~JSONParser()
{
}



void JSONParser::handle_data(const char *s, int len)
{
  json_buffer.append(s, len); // check for problems with null termination FIXME
  buf_len += len;
}

// parse a supplied JSON fragment
bool JSONParser::parse(const char *buf_, int len)
{
  if (!buf_) {
    set_failure();
    return false;
  }

  string json_string(buf_, len);
  success = read(json_string, data);
  if (success)
    handle_value(data);
  else
    set_failure();

  return success;
}

// parse the internal json_buffer up to len
bool JSONParser::parse(int len)
{
  string json_string = json_buffer.substr(0, len);
  success = read(json_string, data);
  if (success)
    handle_value(data);
  else
    set_failure();

  return success;
}

// parse the complete internal json_buffer
bool JSONParser::parse()
{
  success = read(json_buffer, data);
  if (success)
    handle_value(data);
  else
    set_failure();

  return success;
}

// parse a supplied ifstream, for testing. DELETE ME
bool JSONParser::parse(const char *file_name)
{
  ifstream is(file_name);
  success = read(is, data);
  if (success)
    handle_value(data);
  else
    set_failure();

  return success;
}


void decode_json_obj(long& val, JSONObj *oid)
{
  string s = oid->get_data();
  const char *start = s.c_str();
  char *p;

  errno = 0;
  val = strtol(start, &p, 10);

  /* Check for various possible errors */

 if ((errno == ERANGE && (val == LONG_MAX || val == LONG_MIN)) ||
     (errno != 0 && val == 0)) {
   throw JSONDecoder::err("failed to parse number");
 }

 if (p == start) {
   throw JSONDecoder::err("failed to parse number");
 }

 while (*p != '\0') {
   if (!isspace(*p)) {
     throw JSONDecoder::err("failed to parse number");
   }
   p++;
 }
}

void decode_json_obj(unsigned long& val, JSONObj *oid)
{
  string s = oid->get_data();
  const char *start = s.c_str();
  char *p;

  errno = 0;
  val = strtoul(start, &p, 10);

  /* Check for various possible errors */

 if ((errno == ERANGE && val == ULONG_MAX) ||
     (errno != 0 && val == 0)) {
   throw JSONDecoder::err("failed to number");
 }

 if (p == start) {
   throw JSONDecoder::err("failed to parse number");
 }

 while (*p != '\0') {
   if (!isspace(*p)) {
     throw JSONDecoder::err("failed to parse number");
   }
   p++;
 }
}

void decode_json_obj(long long& val, JSONObj *oid)
{
  string s = oid->get_data();
  const char *start = s.c_str();
  char *p;

  errno = 0;
  val = strtoll(start, &p, 10);

  /* Check for various possible errors */

 if ((errno == ERANGE && (val == LLONG_MAX || val == LLONG_MIN)) ||
     (errno != 0 && val == 0)) {
   throw JSONDecoder::err("failed to parse number");
 }

 if (p == start) {
   throw JSONDecoder::err("failed to parse number");
 }

 while (*p != '\0') {
   if (!isspace(*p)) {
     throw JSONDecoder::err("failed to parse number");
   }
   p++;
 }
}

void decode_json_obj(unsigned long long& val, JSONObj *oid)
{
  string s = oid->get_data();
  const char *start = s.c_str();
  char *p;

  errno = 0;
  val = strtoull(start, &p, 10);

  /* Check for various possible errors */

 if ((errno == ERANGE && val == ULLONG_MAX) ||
     (errno != 0 && val == 0)) {
   throw JSONDecoder::err("failed to number");
 }

 if (p == start) {
   throw JSONDecoder::err("failed to parse number");
 }

 while (*p != '\0') {
   if (!isspace(*p)) {
     throw JSONDecoder::err("failed to parse number");
   }
   p++;
 }
}

void decode_json_obj(int& val, JSONObj *oid)
{
  long l;
  decode_json_obj(l, oid);
#if LONG_MAX > INT_MAX
  if (l > INT_MAX || l < INT_MIN) {
    throw JSONDecoder::err("integer out of range");
  }
#endif

  val = (int)l;
}

void decode_json_obj(unsigned& val, JSONObj *oid)
{
  unsigned long l;
  decode_json_obj(l, oid);
#if ULONG_MAX > UINT_MAX
  if (l > UINT_MAX) {
    throw JSONDecoder::err("unsigned integer out of range");
  }
#endif

  val = (unsigned)l;
}

void decode_json_obj(bool& val, JSONObj *oid)
{
  string s = oid->get_data();
  if (strcasecmp(s.c_str(), "true") == 0) {
    val = true;
    return;
  }
  if (strcasecmp(s.c_str(), "false") == 0) {
    val = false;
    return;
  }
  int i;
  decode_json_obj(i, oid);
  val = (bool)i;
}

void decode_json_obj(bufferlist& val, JSONObj *oid)
{
  string s = oid->get_data();

  bufferlist bl;
  bl.append(s.c_str(), s.size());
  val.decode_base64(bl);
}

void decode_json_obj(ceph::real_time& val, JSONObj *oid)
{
  string s = oid->get_data();
  struct tm tm;
  memset(&tm, 0, sizeof(tm));
  val = ceph::real_time::min();

  const char *p = strptime(s.c_str(), "%Y-%m-%d", &tm);
  if (p) {
    if (*p == ' ') {
      p++;
      p = strptime(p, " %H:%M:%S", &tm);
      if (!p)
	throw JSONDecoder::err("failed to decode ceph::real_time");
      if (*p == '.') {
	++p;
	unsigned i;
	char buf[10]; /* 9 digit + null termination */
	for (i = 0; (i < sizeof(buf) - 1) && isdigit(*p); ++i, ++p) {
	  buf[i] = *p;
	}
	for (; i < sizeof(buf) - 1; ++i) {
	  buf[i] = '0';
	}
	buf[i] = '\0';
	string err;
	val += ceph::timespan((ceph_timerep)strict_strtol(buf, 10, &err));
	if (!err.empty()) {
	  throw JSONDecoder::err("failed to decode ceph::real_time");
	}
      }
    }
  } else {
    int sec, usec;
    int r = sscanf(s.c_str(), "%d.%d", &sec, &usec);
    if (r != 2) {
      throw JSONDecoder::err("failed to decode ceph::real_time");
    }

    time_t tt = sec;
    gmtime_r(&tt, &tm);

    val += std::chrono::microseconds(usec);
    time_t t = timegm(&tm);
    val += std::chrono::seconds(t);
  }
}

void encode_json(const char *name, const string& val, Formatter *f)
{
  f->dump_string(name, val);
}

void encode_json(const char *name, const char *val, Formatter *f)
{
  f->dump_string(name, val);
}

void encode_json(const char *name, bool val, Formatter *f)
{
  string s;
  if (val)
    s = "true";
  else
    s = "false";

  f->dump_string(name, s);
}

void encode_json(const char *name, int val, Formatter *f)
{
  f->dump_int(name, val);
}

void encode_json(const char *name, long val, Formatter *f)
{
  f->dump_int(name, val);
}

void encode_json(const char *name, unsigned val, Formatter *f)
{
  f->dump_unsigned(name, val);
}

void encode_json(const char *name, unsigned long val, Formatter *f)
{
  f->dump_unsigned(name, val);
}

void encode_json(const char *name, unsigned long long val, Formatter *f)
{
  f->dump_unsigned(name, val);
}

void encode_json(const char *name, long long val, Formatter *f)
{
  f->dump_int(name, val);
}

void encode_json(const char *name, const ceph::real_time& val, Formatter *f)
{
  f->dump_stream(name) << val;
}

void encode_json(const char *name, const bufferlist& bl, Formatter *f)
{
  /* need to copy data from bl, as it is const bufferlist */
  bufferlist src = bl;

  bufferlist b64;
  src.encode_base64(b64);

  string s(b64.c_str(), b64.length());

  encode_json(name, s, f);
}

