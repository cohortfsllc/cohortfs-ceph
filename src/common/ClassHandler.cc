
#include "include/types.h"
#include "msg/Message.h"
#include "osd/OSD.h"
#include "messages/MClass.h"
#include "ClassHandler.h"
#include "common/arch.h"

#include <dlfcn.h>

#include <map>

#include "common/config.h"

#define DOUT_SUBSYS osd
#undef dout_prefix
#define dout_prefix *_dout

static ClassHandler::ClassData null_cls_data;

int ClassHandler::_load_class(ClassData &cls)
{
  int ret;
  dout(10) << "load_class " << cls.name << dendl;

  cls_deps_t *(*cls_deps)();

  char fname[80];
  snprintf(fname, sizeof(fname), "%s/class-XXXXXX",
	   g_conf.osd_class_tmp.c_str());

  int fd = mkstemp(fname);
  if (fd < 0) {
   dout(0) << "could not create temp file " << fname << dendl;
   return -errno;
  }
  cls.impl.binary.write_fd(fd);
  close(fd);

  cls.handle = dlopen(fname, RTLD_NOW);

  if (!cls.handle) {
    dout(0) << "could not open class (dlopen failed): " << dlerror() << dendl;
    ret = -EIO;
    goto done;
  }
  cls_deps = (cls_deps_t *(*)())dlsym(cls.handle, "class_deps");
  if (cls_deps) {
    cls_deps_t *deps = cls_deps();
    while (deps) {
      if (!deps->name)
        break;
      cls._add_dependency(deps);
      deps++;
    }
  }
  ret = cls.load();
done:
  unlink(fname);
  return ret;
}

int ClassHandler::load_class(const string& cname)
{
  int ret;

  ClassData& cls = get_obj(cname);
  if (&cls == &null_cls_data) {
    dout(0) << "ERROR: can't load null class data" << dendl;
    return -EINVAL;
  }
  cls.mutex->Lock();
  ret = _load_class(cls);
  cls.mutex->Unlock();

  return ret;
}

ClassHandler::ClassData& ClassHandler::get_obj(const string& cname)
{
  Mutex::Locker locker(mutex);
  map<string, ClassData>::iterator iter = classes.find(cname);
  if (iter == classes.end()) {
    ClassData& cls = classes[cname];
    cls.mutex = new Mutex("ClassData");
    if (!cls.mutex) {
      classes[cname] = null_cls_data;
      return null_cls_data;
    }
    dout(0) << "get_obj: adding new class name=" << cname << " ptr=" << &cls << dendl;
    cls.name = cname;
    cls.osd = osd;
    cls.handler = this;
    return cls;
  }

  return iter->second;
}

ClassHandler::ClassData *ClassHandler::get_class(const string& cname, ClassVersion& version)
{
  ClassData *ret = NULL;
  ClassData *cls = &get_obj(cname);
  if (cls == &null_cls_data)
    return NULL;

  Mutex::Locker lock(*cls->mutex);

  switch (cls->status) {
  case ClassData::CLASS_LOADED:
  case ClassData::CLASS_ERROR:
  case ClassData::CLASS_INVALID:
    if (cls->cache_timed_out()) {
      dout(0) << "class timed out going to send request for " << cname.c_str() << " v" << version << dendl;
      ret = cls;
      goto send;
    }
    return cls;

  case ClassData::CLASS_REQUESTED:
    return NULL;

  case ClassData::CLASS_UNKNOWN:
    cls->set_status(ClassData::CLASS_REQUESTED);
    break;

  default:
    assert(0);
  }

  cls->version = version;
send:
  osd->send_class_request(cname.c_str(), version);
  return ret;
}

void ClassHandler::handle_class(MClass *m)
{
  deque<ClassInfo>::iterator info_iter;
  deque<ClassImpl>::iterator impl_iter;
  deque<bool>::iterator add_iter;
  
  for (info_iter = m->info.begin(), add_iter = m->add.begin(), impl_iter = m->impl.begin();
       info_iter != m->info.end();
       ++info_iter, ++add_iter) {
    ClassData& data = get_obj(info_iter->name);
    if (&data == &null_cls_data) {
      dout(1) << "couldn't get class, out of memory? continuing" << dendl;
      continue;
    }
    dout(10) << "handle_class " << info_iter->name << dendl;
    data.mutex->Lock();
    
    if (*add_iter) {
      data.set_status(ClassData::CLASS_REQUESTED);
      dout(10) << "added class '" << info_iter->name << "'" << dendl;
      data.impl = *impl_iter;
      ++impl_iter;
      int ret = _load_class(data);
      if (ret < 0) {
	data.set_status(ClassData::CLASS_ERROR);
	osd->got_class(info_iter->name);
      }
    } else {
      dout(10) << "response of an invalid class '" << info_iter->name << "'" << dendl;
      data.set_status(ClassData::CLASS_INVALID);
      osd->got_class(info_iter->name);
    }
    data.mutex->Unlock();
  }
}


void ClassHandler::resend_class_requests()
{
  for (map<string,ClassData>::iterator p = classes.begin(); p != classes.end(); p++) {
    dout(20) << "resending class request "<< p->first.c_str() << " v" << p->second.version << dendl;
    osd->send_class_request(p->first.c_str(), p->second.version);
  }
}

ClassHandler::ClassData *ClassHandler::register_class(const char *cname)
{
  ClassData& class_data = get_obj(cname);

  if (&class_data == &null_cls_data) {
    dout(0) << "couldn't get class object, out of memory?" << dendl;
    return NULL;
  }

  dout(0) << "&class_data=" << (void *)&class_data << " status=" << class_data.status << dendl;

  if (class_data.status != ClassData::CLASS_LOADED) {
    dout(0) << "class " << cname << " can't be loaded" << dendl;
    return NULL;
  }

  if (class_data.registered) {
    dout(0) << "class " << cname << " already registered" << dendl;
  }

  class_data.registered = true;

  return &class_data;
}

void ClassHandler::unregister_class(ClassHandler::ClassData *cls)
{
  /* FIXME: do we really need this one? */
}


int ClassHandler::ClassData::load()
{
  int ret;
  switch (status) {
    case CLASS_INVALID:
      ret = -EINVAL;
      break;
    case CLASS_ERROR:
      ret = -EIO;
      break;
    default:
      ret = 0;
  }
  if (ret) {
    /* if we're invalid, we should just notify osd */
    osd->got_class(name);
    return ret;
  }

  if (!has_missing_deps()) {
    set_status(CLASS_LOADED);
    dout(0) << "setting class " << name << " status to CLASS_LOADED" << dendl;
    init();
    osd->got_class(name);
  }

  list<ClassData *>::iterator iter;
  for (iter = dependents.begin(); iter != dependents.end(); ++iter) {
    ClassData *cls = *iter;
    cls->satisfy_dependency(this);
  }

  return 0;
}

void ClassHandler::ClassData::init()
{
  void (*cls_init)() = (void (*)())dlsym(handle, "__cls_init");

  if (cls_init)
    cls_init();
}

bool ClassHandler::ClassData::_add_dependency(cls_deps_t *dep)
{
  if (!dep->name)
    return false;

  ClassData& cls_dep = handler->get_obj(dep->name);
  if (&cls_dep == &null_cls_data) {
    dout(0) << "couldn't get class dep object, out of memory?" << dendl;
    return false;
  }
  map<string, ClassData *>::iterator iter = missing_dependencies.find(dep->name);
  dependencies[dep->name] = &cls_dep;
  dout(0) << "adding dependency " << dep->name << dendl;

  if (cls_dep.status != CLASS_LOADED) {
    missing_dependencies[dep->name] = &cls_dep;

    if(cls_dep.status == CLASS_UNKNOWN) {
      ClassVersion version;
      version.set_arch(get_arch());
      handler->get_class(dep->name, version);
    }
    dout(0) << "adding missing dependency " << dep->name << dendl;
  }
  cls_dep._add_dependent(*this);

  if ((cls_dep.status == CLASS_INVALID) ||
      (cls_dep.status == CLASS_ERROR))  {
    dout(0) << "ouch! depending on bad class" << dendl;
    set_status(cls_dep.status); /* we have an invalid dependency, we're invalid */
  }

  return true;
}

void ClassHandler::ClassData::satisfy_dependency(ClassData *cls)
{
  Mutex::Locker lock(*mutex);
  map<string, ClassData *>::iterator iter = missing_dependencies.find(cls->name);

  if (iter != missing_dependencies.end()) {
    dout(0) << "satisfied dependency name=" << name << " dep=" << cls->name << dendl;
    missing_dependencies.erase(iter);
    if (missing_dependencies.size() == 0) {
      dout(0) << "all dependencies are satisfied! initializing, notifying osd" << dendl;
      set_status(CLASS_LOADED);
  dout(0) << "this=" << (void *)this << " status=" << status << dendl;
      init();
      osd->got_class(name);
    }
  }
}

void ClassHandler::ClassData::_add_dependent(ClassData& dependent)
{
  Mutex::Locker lock(*mutex);
  dout(0) << "class " << name << " has dependet: " << dependent.name << dendl;
  dependents.push_back(&dependent);
}

ClassHandler::ClassMethod *ClassHandler::ClassData::register_method(const char *mname,
                                                                    int flags,
								    cls_method_call_t func)
{
  /* no need for locking, called under the class_init mutex */
  ClassMethod& method = methods_map[mname];
  method.func = func;
  method.name = mname;
  method.flags = flags;
  method.cls = this;

  return &method;
}

ClassHandler::ClassMethod *ClassHandler::ClassData::register_cxx_method(const char *mname,
                                                                        int flags,
									cls_method_cxx_call_t func)
{
  /* no need for locking, called under the class_init mutex */
  ClassMethod& method = methods_map[mname];
  method.cxx_func = func;
  method.name = mname;
  method.flags = flags;
  method.cls = this;

  return &method;
}

ClassHandler::ClassMethod *ClassHandler::ClassData::get_method(const char *mname)
{
  Mutex::Locker lock(*mutex);
  map<string, ClassHandler::ClassMethod>::iterator iter = methods_map.find(mname);

  if (iter == methods_map.end())
    return NULL;

  return &(iter->second);
}

void ClassHandler::ClassData::unregister_method(ClassHandler::ClassMethod *method)
{
 /* no need for locking, called under the class_init mutex */
   map<string, ClassMethod>::iterator iter;

   iter = methods_map.find(method->name);
   if (iter == methods_map.end())
     return;

   methods_map.erase(iter);
}

void ClassHandler::ClassData::set_status(ClassHandler::ClassData::Status _status)
{
  status = _status;

  utime_t e = g_clock.now();
  switch (status) {
    case CLASS_ERROR:
    case CLASS_INVALID:
      e += g_conf.osd_class_error_timeout;
      expires = e;
      break;

    case CLASS_LOADED:
      e += g_conf.osd_class_timeout;
      expires = e;
      break;

    default:
      break;
  }
}

bool ClassHandler::ClassData::cache_timed_out()
{
  utime_t now = g_clock.now();
  dout(0) << "expires " << expires << " now " << now << dendl;
  return expires != utime_t() && now > expires;
}

void ClassHandler::ClassMethod::unregister()
{
  cls->unregister_method(this);
}

int ClassHandler::ClassMethod::exec(cls_method_context_t ctx, bufferlist& indata, bufferlist& outdata)
{
  int ret;
  if (cxx_func) {
    // C++ call version
    ret = cxx_func(ctx, &indata, &outdata);
  } else {
    // C version
    char *out = NULL;
    int olen = 0;
    ret = func(ctx, indata.c_str(), indata.length(), &out, &olen);
    if (out) {
      // assume *out was allocated via cls_alloc (which calls malloc!)
      buffer::ptr bp = buffer::claim_malloc(olen, out);
      outdata.push_back(bp);
    }
  }
  return ret;
}

int ClassHandler::get_method_flags(const string& cname, const string& mname)
{
  ClassData& cls = get_obj(cname);
  if (&cls == &null_cls_data)
    return 0;

  ClassMethod *method = cls.get_method(mname.c_str());
  if (!method)
    return 0;

  return method->flags;
}
