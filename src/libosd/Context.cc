// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "Context.h"

#include "common/debug.h"
#include "common/common_init.h"
#include "common/config.h"
#include "common/ceph_argparse.h"
#include "global/global_context.h"

#define dout_subsys ceph_subsys_osd

namespace
{

// Maintain a set of ceph contexts so we can make sure that
// g_ceph_context and g_conf are always valid, if available
class ContextSet {
  Mutex mtx;
  std::set<CephContext*> contexts;
public:
  void insert(CephContext *cct) {
    Mutex::Locker lock(mtx);
    contexts.insert(cct);
    // initialize g_ceph_context
    if (g_ceph_context == nullptr) {
      g_ceph_context = cct;
      g_conf = cct->_conf;
    }
  }
  void erase(CephContext *cct) {
    Mutex::Locker lock(mtx);
    contexts.erase(cct);
    // replace g_ceph_context
    if (g_ceph_context == cct) {
      g_ceph_context = contexts.empty() ? nullptr : *contexts.begin();
      g_conf = g_ceph_context ? g_ceph_context->_conf : nullptr;
    }
  }
};

ContextSet contexts;

} // anonymous namespace

namespace ceph
{
namespace osd
{

Context::~Context()
{
  if (cct) {
    cct->put();
    contexts.erase(cct);
  }
}

int Context::create(int id, const char *config, const char *cluster)
{
  CephInitParameters params(CEPH_ENTITY_TYPE_OSD);
  char name[12];
  sprintf(name, "%d", id);
  params.name.set_id(name);

  cct = common_preinit(params, CODE_ENVIRONMENT_DAEMON, 0);
  contexts.insert(cct);

  conf = cct->_conf;

  if (cluster && cluster[0])
    conf->cluster.assign(cluster);

  // parse configuration
  std::deque<std::string> parse_errors;
  int r = conf->parse_config_files(config, &parse_errors, &cerr, 0);
  if (r != 0) {
    derr << "failed to parse configuration " << config << dendl;
    return r;
  }
  conf->apply_changes(nullptr);
  complain_about_parse_errors(cct, &parse_errors);

  cct->init();
  return 0;
}

} // namespace osd
} // namespace ceph
