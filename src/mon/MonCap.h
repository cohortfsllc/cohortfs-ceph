// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_MONCAP_H
#define CEPH_MONCAP_H

#include <ostream>
using std::ostream;

#include "include/types.h"
#include "msg/msg_types.h"

class CephContext;

static const uint8_t MON_CAP_R     = (1 << 1);      // read
static const uint8_t MON_CAP_W     = (1 << 2);      // write
static const uint8_t MON_CAP_X     = (1 << 3);      // execute
static const uint8_t MON_CAP_ALL   = MON_CAP_R | MON_CAP_W | MON_CAP_X;
static const uint8_t MON_CAP_ANY   = 0xff;	    // *

struct mon_rwxa_t {
  uint8_t val;

  mon_rwxa_t(uint8_t v = 0) : val(v) {}
  mon_rwxa_t& operator=(uint8_t v) {
    val = v;
    return *this;
  }
  operator uint8_t() const {
    return val;
  }
};

ostream& operator<<(ostream& out, mon_rwxa_t p);

struct StringConstraint {
  string value;
  string prefix;

  StringConstraint() {}
  StringConstraint(string a, string b) : value(a), prefix(b) {}
};

ostream& operator<<(ostream& out, const StringConstraint& c);

struct MonCapGrant {
  /*
   * A grant can come in one of four forms:
   *
   *  - a blanket allow ('allow rw', 'allow *')
   *    - this will match against any service and the read/write/exec flags
   *      in the mon code.  semantics of what X means are somewhat ad hoc.
   *
   *  - a service allow ('allow service mds rw')
   *    - this will match against a specific service and the r/w/x flags.
   *
   *  - a profile ('allow profile osd')
   *    - this will match against specific monitor-enforced semantics of what
   *      this type of user should need to do.  examples include 'osd', 'mds',
   *      'bootstrap-osd'.
   *
   *  - a command ('allow command foo', 'allow command bar with arg1=val1 arg2 prefix val2')
   *      this includes the command name (the prefix string), and a set
   *      of key/value pairs that constrain use of that command.  if no pairs
   *      are specified, any arguments are allowed; if a pair is specified, that
   *      argument must be present and equal or match a prefix.
   */
  std::string service;
  std::string profile;
  std::string command;
  map<std::string,StringConstraint> command_args;

  mon_rwxa_t allow;

  // explicit grants that a profile grant expands to; populated as
  // needed by expand_profile() (via is_match()) and cached here.
  mutable list<MonCapGrant> profile_grants;

  void expand_profile(entity_name_t name) const;

  MonCapGrant() : allow(0) {}
  MonCapGrant(mon_rwxa_t a) : allow(a) {}
  MonCapGrant(string s, mon_rwxa_t a) : service(s), allow(a) {}
  MonCapGrant(string c) : command(c) {}
  MonCapGrant(string c, string a, StringConstraint co) : command(c) {
    command_args[a] = co;
  }

  /**
   * check if given request parameters match our constraints
   *
   * @param cct context
   * @param name entity name
   * @param service service (if any)
   * @param command command (if any)
   * @param command_args command args (if any)
   * @return bits we allow
   */
  mon_rwxa_t get_allowed(CephContext *cct,
			 entity_name_t name,
			 const std::string& service,
			 const std::string& command,
			 const map<string,string>& command_args) const;

  bool is_allow_all() const {
    return
      allow == MON_CAP_ANY &&
      service.length() == 0 &&
      profile.length() == 0 &&
      command.length() == 0;
  }
};

ostream& operator<<(ostream& out, const MonCapGrant& g);

struct MonCap {
  string text;
  std::vector<MonCapGrant> grants;

  MonCap() {}
  MonCap(std::vector<MonCapGrant> g) : grants(g) {}

  string get_str() const {
    return text;
  }

  bool is_allow_all() const;
  void set_allow_all();
  bool parse(const std::string& str, ostream *err=NULL);

  /**
   * check if we are capable of something
   *
   * This method actually checks a description of a particular operation against
   * what the capability has specified.
   *
   * @param service service name
   * @param command command id
   * @param command_args
   * @param op_may_read whether the operation may need to read
   * @param op_may_write whether the operation may need to write
   * @param op_may_exec whether the operation may exec
   * @return true if the operation is allowed, false otherwise
   */
  bool is_capable(CephContext *cct,
		  entity_name_t name,
		  const string& service,
		  const string& command, const map<string,string>& command_args,
		  bool op_may_read, bool op_may_write, bool op_may_exec) const;

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& bl);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<MonCap*>& ls);
};
WRITE_CLASS_ENCODER(MonCap)

ostream& operator<<(ostream& out, const MonCap& cap);

static inline bool is_not_alnum_space(char c)
{
  return !(isalpha(c) || isdigit(c) || (c == '-') || (c == '_'));
}

static string maybe_quote_string(const std::string& str)
{
  if (find_if(str.begin(), str.end(), is_not_alnum_space) == str.end())
    return str;
  return string("\"") + str + string("\"");
}

template <typename T>
T& operator<<(T& out, mon_rwxa_t p)
{
  if (p == MON_CAP_ANY)
    return out << "*";

  if (p & MON_CAP_R)
    out << "r";
  if (p & MON_CAP_W)
    out << "w";
  if (p & MON_CAP_X)
    out << "x";
  return out;
}

template <typename T>
T& operator<<(T& out, const StringConstraint& c)
{
  if (c.prefix.length())
    return out << "prefix " << c.prefix;
  else
    return out << "value " << c.value;
}

template <typename T>
T& operator<<(T& out, const MonCapGrant& m)
{
  out << "allow";
  if (m.service.length()) {
    out << " service " << maybe_quote_string(m.service);
  }
  if (m.command.length()) {
    out << " command " << maybe_quote_string(m.command);
    if (!m.command_args.empty()) {
      out << " with";
      for (map<string,StringConstraint>::const_iterator p = m.command_args.begin();
	   p != m.command_args.end();
	   ++p) {
	if (p->second.value.length())
	  out << " " << maybe_quote_string(p->first) << "=" << maybe_quote_string(p->second.value);
	else
	  out << " " << maybe_quote_string(p->first) << " prefix " << maybe_quote_string(p->second.prefix);
      }
    }
  }
  if (m.profile.length()) {
    out << " profile " << maybe_quote_string(m.profile);
  }
  if (m.allow != 0)
    out << " " << m.allow;
  return out;
}

template <typename T>
T& operator<<(T&out, const MonCap& m)
{
  for (vector<MonCapGrant>::const_iterator p = m.grants.begin(); p != m.grants.end(); ++p) {
    if (p != m.grants.begin())
      out << ", ";
    out << *p;
  }
  return out;
}

#endif
