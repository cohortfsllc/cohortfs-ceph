#ifndef CEPH_CDS_ENV_H
#define CEPH_CDS_ENV_H

#include "acconfig.h"

#ifdef HAVE_CDS

#include <cds/init.h>  //cds::Initialize и cds::Terminate
#include <cds/gc/hp.h> //cds::gc::HP (Hazard Pointer)
#include <cds/intrusive/skip_list_hp.h> //cds intrusive skip lists

class CDS_Env {
    cds::gc::HP hpGC;
public:
    CDS_Env() : hpGC(167) {
	cds::Initialize(0);
	cds::threading::Manager::init();
    }
};

#else // HAVE_CDS

class CDS_Env {};

#endif

#endif /* CEPH_CDS_ENV_H */