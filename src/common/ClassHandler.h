#ifndef CEPH_CLASSHANDLER_H
#define CEPH_CLASSHANDLER_H

#include "include/types.h"
#include "include/ClassLibrary.h"

#include "objclass/objclass.h"

#include "common/Cond.h"
#include "common/Mutex.h"
#include "common/ClassVersion.h"


class OSD;
class MClass;



class ClassHandler
{
  OSD *osd;

public:
  class ClassData;

  struct ClassMethod {
    struct ClassHandler::ClassData *cls;
    string name;
    int flags;
    cls_method_call_t func;
    cls_method_cxx_call_t cxx_func;

    int exec(cls_method_context_t ctx, bufferlist& indata, bufferlist& outdata);
    void unregister();

    ClassMethod() : cls(0), func(0), cxx_func(0) {}
  };

  struct ClassData {
    Mutex *mutex;
    bool _add_dependency(cls_deps_t *dep);
    void _add_dependent(ClassData& dependent);

    void satisfy_dependency(ClassData *cls);

    enum Status { 
      CLASS_UNKNOWN,
      CLASS_INVALID,
      CLASS_ERROR,
      CLASS_LOADED,
      CLASS_REQUESTED,
    } status;
    ClassVersion version;
    utime_t expires;
    ClassImpl impl;
    string name;
    OSD *osd;
    ClassHandler *handler;
    void *handle;
    bool registered;
    map<string, ClassMethod> methods_map;

    map<string, ClassData *> dependencies; /* our dependencies */
    map<string, ClassData *> missing_dependencies; /* only missing dependencies */
    list<ClassData *> dependents;          /* classes that depend on us */

    bool has_missing_deps() { return (missing_dependencies.size() > 0); }

    ClassData() : mutex(NULL), status(CLASS_UNKNOWN), version(), handle(NULL), registered(false)  {}
    ~ClassData() { if (mutex) delete mutex; }

    ClassMethod *register_method(const char *mname, int flags, cls_method_call_t func);
    ClassMethod *register_cxx_method(const char *mname, int flags, cls_method_cxx_call_t func);
    ClassMethod *get_method(const char *mname);
    void unregister_method(ClassMethod *method);

    int load();
    void init();

    void set_status(Status _status);
    bool cache_timed_out();
  };
  Mutex mutex;
  map<string, ClassData> classes;

  ClassData& get_obj(const string& cname);

  int load_class(const string& cname);
  int _load_class(ClassData &data);

  ClassHandler(OSD *_osd) : osd(_osd), mutex("ClassHandler") {}

  ClassData *get_class(const string& cname, ClassVersion& version);
  void resend_class_requests();

  void handle_class(MClass *m);

  ClassData *register_class(const char *cname);
  void unregister_class(ClassData *cls);

  int get_method_flags(const string& cname, const string& mname);
};


#endif
