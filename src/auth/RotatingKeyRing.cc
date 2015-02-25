// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include <errno.h>
#include <map>

#include "common/ceph_context.h"
#include "common/config.h"
#include "common/debug.h"
#include "include/str_list.h"

#include "Crypto.h"
#include "auth/RotatingKeyRing.h"
#include "auth/KeyRing.h"

#define dout_subsys ceph_subsys_auth
#undef dout_prefix
#define dout_prefix *_dout << "auth: "


bool RotatingKeyRing::need_new_secrets() const
{
  lock_guard l(lock);
  return secrets.need_new_secrets();
}

bool RotatingKeyRing::need_new_secrets(ceph::real_time now) const
{
  lock_guard l(lock);
  return secrets.need_new_secrets(now);
}

void RotatingKeyRing::set_secrets(RotatingSecrets& s)
{
  lock_guard l(lock);
  secrets = s;
  dump_rotating();
}

void RotatingKeyRing::dump_rotating() const
{
  ldout(cct, 10) << "dump_rotating:" << dendl;
  for (map<uint64_t, ExpiringCryptoKey>::const_iterator iter = secrets.secrets.begin();
       iter != secrets.secrets.end();
       ++iter)
    ldout(cct, 10) << " id " << iter->first << " " << iter->second << dendl;
}

bool RotatingKeyRing::get_secret(const EntityName& name, CryptoKey& secret) const
{
  lock_guard l(lock);
  return keyring->get_secret(name, secret);
}

bool RotatingKeyRing::get_service_secret(uint32_t service_id_, uint64_t secret_id,
					 CryptoKey& secret) const
{
  lock_guard l(lock);

  if (service_id_ != this->service_id) {
    ldout(cct, 0) << "do not have service " << ceph_entity_type_name(service_id_)
	    << ", i am " << ceph_entity_type_name(this->service_id) << dendl;
    return false;
  }

  map<uint64_t, ExpiringCryptoKey>::const_iterator iter =
    secrets.secrets.find(secret_id);
  if (iter == secrets.secrets.end()) {
    ldout(cct, 0) << "could not find secret_id=" << secret_id << dendl;
    dump_rotating();
    return false;
  }

  secret = iter->second.key;
  return true;
}

KeyRing *RotatingKeyRing::
get_keyring()
{
  return keyring;
}
