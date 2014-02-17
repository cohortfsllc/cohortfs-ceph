// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-

#ifndef CEPH_COHORTOSD_H
#define CEPH_COHORTOSD_H

#include "include/types.h"

// stl
#include <string>
#include <set>
#include <map>
#include <fstream>
using std::set;
using std::map;
using std::fstream;

#include "include/filepath.h"
#include "include/interval_set.h"
#include "include/lru.h"

#include "common/Mutex.h"
#include "common/Timer.h"
#include "common/Finisher.h"

#include "common/compiler_extensions.h"


#include "osd/OSD.h"
#include "common/AsyncReserver.h"
#include "cohort/CohortOSDMap.h"
#include "OSDVol.h"

class CohortOSD;
class CohortOSDService;

class CohortOSDService : public OSDService {
private:

  typedef OSDService inherited;

public:

  virtual bool test_ops_sub(ObjectStore *store,
			    std::string command,
			    std::string args,
			    ostream &ss);

  void init_sub();
  void shutdown_sub();

  CohortOSD* cohortosd() const;

  CohortOSDService(CohortOSD *osd);

  virtual OSDMapRef newOSDMap() const {
    return OSDMapRef(new CohortOSDMap());
  }
  CohortOSDMapRef get_map(epoch_t e) {
    return static_pointer_cast<const CohortOSDMap>(get_map(e));
  }

  const CohortOSDMapRef cohortosdmap() {
    return dynamic_pointer_cast<const CohortOSDMap>(get_osdmap());
  }
  // -- Watch --
  Mutex watch_lock;
  SafeTimer watch_timer;
  uint64_t next_notif_id;
  uint64_t get_next_id(epoch_t cur_epoch) {
    Mutex::Locker l(watch_lock);
    return (((uint64_t)cur_epoch) << 32) | ((uint64_t)(next_notif_id++));
  }
  ThreadPool::WorkQueueVal<pair<OSDVolRef, OpRequestRef>, OSDVolRef> &op_wq;
  ThreadPool::WorkQueue<OSDVol> &snap_trim_wq;

  bool queue_for_snap_trim(OSDVol *vol) {
    return snap_trim_wq.queue(vol);
  }

};

typedef shared_ptr<CohortOSDService> CohortOSDServiceRef;


class CohortOSD : public OSD {
  friend CohortOSDService;

  OSDVolRef get_volume(const uuid_d volid) {
    return OSDVolRef();
  }
private:

  typedef OSD inherited;

  CohortOSDMapRef get_osdmap_with_maplock() const {
    assert(osdmap);
    return static_pointer_cast<const CohortOSDMap>(osdmap);
  }

  map<uuid_d, OSDVolRef> vol_map;
  list<OpRequestRef> waiting_for_map;

  struct OpWQ: public ThreadPool::WorkQueueVal<pair<OSDVolRef, OpRequestRef>,
					       OSDVolRef> {
    Mutex qlock;
    map<OSDVolRef, list<OpRequestRef> > vol_for_processing;
    CohortOSD *osd;
    PrioritizedQueue<pair<OSDVolRef, OpRequestRef>, entity_inst_t > pqueue;
    OpWQ(CohortOSD *o, time_t ti, ThreadPool *tp)
      : ThreadPool::WorkQueueVal<pair<OSDVolRef, OpRequestRef>, OSDVolRef >(
	"CohortOSD::OpWQ", ti, ti*10, tp),
	qlock("OpWQ::qlock"),
	osd(o),
	pqueue(o->cct->_conf->osd_op_pq_max_tokens_per_priority,
	       o->cct->_conf->osd_op_pq_min_cost)
      {}

    void dump(Formatter *f) {
      Mutex::Locker l(qlock);
      pqueue.dump(f);
    }

    void _enqueue_front(pair<OSDVolRef, OpRequestRef> item);
    void _enqueue(pair<OSDVolRef, OpRequestRef> item);
    OSDVolRef _dequeue();

    struct Pred {
      OSDVolRef vol;
      Pred(OSDVolRef vol) : vol(vol) {}
      bool operator()(const pair<OSDVolRef, OpRequestRef> &op) {
	return op.first == vol;
      }
    };
    void dequeue(OSDVolRef vol, list<OpRequestRef> *dequeued = 0) {
      lock();
      if (!dequeued) {
	pqueue.remove_by_filter(Pred(vol));
	vol_for_processing.erase(vol);
      } else {
	list<pair<OSDVolRef, OpRequestRef> > _dequeued;
	pqueue.remove_by_filter(Pred(vol), &_dequeued);
	for (list<pair<OSDVolRef, OpRequestRef> >::iterator i
	       = _dequeued.begin();
	     i != _dequeued.end();
	     ++i) {
	  dequeued->push_back(i->second);
	}
	if (vol_for_processing.count(vol)) {
	  dequeued->splice(dequeued->begin(),
			   vol_for_processing[vol]);
	  vol_for_processing.erase(vol);
	}
      }
      unlock();
    }
    bool _empty() {
      return pqueue.empty();
    }
    void _process(OSDVolRef vol);
  } op_wq;

  void enqueue_op(OSDVolRef vol, OpRequestRef op);
  void dequeue_op(OSDVolRef vol, OpRequestRef op);

  xlist<OSDVol*> snap_trim_queue;


  struct SnapTrimWQ : public ThreadPool::WorkQueue<OSDVol> {
    CohortOSD *osd;
    SnapTrimWQ(CohortOSD *o, time_t ti, ThreadPool *tp)
      : ThreadPool::WorkQueue<OSDVol>("OSD::SnapTrimWQ", ti, 0, tp), osd(o) {}

    bool _empty() {
      return osd->snap_trim_queue.empty();
    }
    bool _enqueue(OSDVol *vol) {
      if (vol->snap_trim_item.is_on_list())
	return false;
      osd->snap_trim_queue.push_back(&vol->snap_trim_item);
      return true;
    }
    void _dequeue(OSDVol *vol) {
      vol->snap_trim_item.remove_myself();
    }
    OSDVol *_dequeue() {
      if (osd->snap_trim_queue.empty())
	return NULL;
      OSDVol *vol = osd->snap_trim_queue.front();
      osd->snap_trim_queue.pop_front();
      return vol;
    }

    void _process(OSDVol *vol) {
      vol->snap_trimmer();
    }
    void _clear() {
      osd->snap_trim_queue.clear();
    }
  } snap_trim_wq;

public:

  virtual void handle_conf_change(const struct md_config_t *conf,
				  const std::set <std::string> &changed);

  CohortOSD(int id, Messenger *internal, Messenger *external,
	    Messenger *hb_client, Messenger *hb_front_server,
	    Messenger *hb_back_server, MonClient *mc,
	    const std::string &dev,
	    const std::string &jdev);

  virtual OSDServiceRef newOSDService(OSD* osd) const {
    return OSDServiceRef(new CohortOSDService(static_cast<CohortOSD*>(osd)));
  }

  virtual int init();
  virtual int shutdown();

  virtual bool do_command_sub(Connection *con,
			      tid_t tid,
			      vector<string>& cmd,
			      bufferlist& data,
			      bufferlist& odata,
			      int& r,
			      ostringstream& ss);
  virtual bool do_command_debug_sub(vector<string>& cmd,
				    int& r,
				    ostringstream& ss);

  bool have_same_or_newer_map(epoch_t e) {
    return e <= osdmap->get_epoch();
  };
  bool op_must_wait_for_map(OpRequestRef op);
  virtual void handle_op_sub(OpRequestRef op);
  virtual bool handle_sub_op_sub(OpRequestRef op);
  virtual bool handle_sub_op_reply_sub(OpRequestRef op);

  virtual void build_heartbeat_peers_list();
  virtual void tick_sub(const utime_t& now);

  virtual void do_mon_report_sub(const utime_t& now);

  virtual void ms_handle_connect_sub(Connection *con);

  virtual void ms_handle_reset_sub(OSD::Session* session);

  virtual void advance_map_sub(ObjectStore::Transaction& t,
			       C_Contexts *tfin);
  virtual void consume_map_sub();
  virtual void dispatch_op_sub(OpRequestRef op);
  virtual bool _dispatch_sub(Message *m);

protected:

  CohortOSDMapRef get_map(epoch_t e) {
    return dynamic_pointer_cast<const CohortOSDMap>(get_map(e));
  }

  const CohortOSDMapRef cohortosdmap() {
    return dynamic_pointer_cast<const CohortOSDMap>(get_osdmap());
  }

  const CohortOSDMapRef cohortosdmap(OSDMapRef ref) const {
    return dynamic_pointer_cast<const CohortOSDMap>(ref);
  }

  const CohortOSDServiceRef cohortosdservice() const {
    return dynamic_pointer_cast<CohortOSDService>(service);
  }

  virtual bool asok_command_sub(string command, string args, ostream& ss);

public:

  OSDMapRef newOSDMap() const {
    return OSDMapRef(new CohortOSDMap());
  }

  void check_replay_queue();
  void sched_scrub();
  OSDVolRef _lookup_vol(uuid_d volid);
};


#endif // CEPH_COHORTOSD_H
