// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_MDS_FLOCK_H
#define CEPH_MDS_FLOCK_H

#include "mdstypes.h"


inline ostream& operator<<(ostream& out, ceph_filelock& l) {
  out << "start: " << l.start << ", length: " << l.length
      << ", client: " << l.client << ", pid: " << l.pid
      << ", type: " << (int)l.type
      << std::endl;
  return out;
}

struct ceph_lock_state_t {
  multimap<uint64_t, ceph_filelock> held_locks;    // current locks
  multimap<uint64_t, ceph_filelock> waiting_locks; // locks waiting for other locks
  // both of the above are keyed by starting offset
  map<client_t, int> client_held_lock_counts;
  map<client_t, int> client_waiting_lock_counts;

  /*
   * Try to set a new lock. If it's blocked and wait_on_fail is true,
   * add the lock to waiting_locks.
   * The lock needs to be of type CEPH_LOCK_EXCL or CEPH_LOCK_SHARED.
   *
   * Returns true if set, false if not set.
   */
  bool add_lock(ceph_filelock& new_lock, bool wait_on_fail) {
    dout(15) << "add_lock " << new_lock << dendl;
    bool ret = false;
    list<multimap<uint64_t, ceph_filelock>::iterator>
      overlapping_locks, self_overlapping_locks, neighbor_locks;
    // first, get any overlapping locks and split them into owned-by-us and not
    if(get_overlapping_locks(new_lock, overlapping_locks, &neighbor_locks)) {
      dout(15) << "got overlapping lock, splitting by owner" << dendl;
      split_by_owner(new_lock, overlapping_locks, self_overlapping_locks);
    }
    if (!overlapping_locks.empty()) { //overlapping locks owned by others :(
      if (CEPH_LOCK_EXCL == new_lock.type) {
	//can't set, we want an exclusive
	dout(15) << "overlapping lock, and this lock is exclusive, can't set"
		<< dendl;
	if (wait_on_fail) {
	  waiting_locks.
	    insert(pair<uint64_t, ceph_filelock>(new_lock.start, new_lock));
	}
	ret = false;
      } else { //shared lock, check for any exclusive locks blocking us
	if (contains_exclusive_lock(overlapping_locks)) { //blocked :(
	  dout(15) << " blocked by exclusive lock in overlapping_locks" << dendl;
	  if (wait_on_fail) {
	    waiting_locks.
	      insert(pair<uint64_t, ceph_filelock>(new_lock.start, new_lock));
	  }
	  ret = false;
	} else {
	  //yay, we can insert a shared lock
	  dout(15) << "inserting shared lock" << dendl;
	  adjust_locks(self_overlapping_locks, new_lock, neighbor_locks);
	  held_locks.
	    insert(pair<uint64_t, ceph_filelock>(new_lock.start, new_lock));
	  ret = true;
	}
      }
    } else { //no overlapping locks except our own
      adjust_locks(self_overlapping_locks, new_lock, neighbor_locks);
      dout(15) << "no conflicts, inserting " << new_lock << dendl;
      held_locks.insert(pair<uint64_t, ceph_filelock>
			(new_lock.start, new_lock));
      ret = true;
    }
    if (ret) ++client_held_lock_counts[(client_t)new_lock.client];
    else if (wait_on_fail) ++client_waiting_lock_counts[(client_t)new_lock.client];
    return ret;
  }

  void look_for_lock(ceph_filelock& testing_lock) {
    list<multimap<uint64_t, ceph_filelock>::iterator> overlapping_locks,
      self_overlapping_locks;
    if (get_overlapping_locks(testing_lock, overlapping_locks)) {
      split_by_owner(testing_lock, overlapping_locks, self_overlapping_locks);
    }
    if (!overlapping_locks.empty()) { //somebody else owns overlapping lock
      if (CEPH_LOCK_EXCL == testing_lock.type) { //any lock blocks it
	testing_lock = (*overlapping_locks.begin())->second;
      } else {
	ceph_filelock *blocking_lock;
	if ((blocking_lock = contains_exclusive_lock(overlapping_locks))) {
	  testing_lock = *blocking_lock;
	} else { //nothing blocking!
	  testing_lock.type = CEPH_LOCK_UNLOCK;
	}
      }
      return;
    }
    //if we get here, only our own locks block
    testing_lock.type = CEPH_LOCK_UNLOCK;
  }

  /*
   * Remove lock(s) described in old_lock. This may involve splitting a
   * previous lock or making a previous lock smaller.
   */
  void remove_lock(ceph_filelock removal_lock,
		   list<ceph_filelock>& activated_locks) {
    list<multimap<uint64_t, ceph_filelock>::iterator> overlapping_locks,
      self_overlapping_locks, crossed_waiting_locks;
    if (get_overlapping_locks(removal_lock, overlapping_locks)) {
      dout(15) << "splitting by owner" << dendl;
      split_by_owner(removal_lock, overlapping_locks, self_overlapping_locks);
    } else dout(15) << "attempt to remove lock at " << removal_lock.start
		   << " but no locks there!" << dendl;
    bool remove_to_end = (0 == removal_lock.length);
    bool old_lock_to_end;
    uint64_t removal_start = removal_lock.start;
    uint64_t removal_end = removal_start + removal_lock.length - 1;
    uint64_t old_lock_end;
    __s64 old_lock_client = 0;
    ceph_filelock *old_lock;

    dout(15) << "examining " << self_overlapping_locks.size()
	    << " self-overlapping locks for removal" << dendl;
    for (list<multimap<uint64_t, ceph_filelock>::iterator>::iterator
	   iter = self_overlapping_locks.begin();
	 iter != self_overlapping_locks.end();
	 ++iter) {
      dout(15) << "self overlapping lock " << (*iter)->second << dendl;
      old_lock = &(*iter)->second;
      old_lock_to_end = (0 == old_lock->length);
      old_lock_end = old_lock->start + old_lock->length - 1;
      old_lock_client = old_lock->client;
      if (remove_to_end) {
	if (old_lock->start < removal_start) {
	  old_lock->length = removal_start - old_lock->start;
	} else {
	  dout(15) << "erasing " << (*iter)->second << dendl;
	  held_locks.erase(*iter);
	  --client_held_lock_counts[old_lock_client];
	}
      } else if (old_lock_to_end) {
	ceph_filelock append_lock = *old_lock;
	append_lock.start = removal_end+1;
	held_locks.insert(pair<uint64_t, ceph_filelock>
			  (append_lock.start, append_lock));
	++client_held_lock_counts[(client_t)old_lock->client];
	if (old_lock->start >= removal_start) {
	  dout(15) << "erasing " << (*iter)->second << dendl;
	  held_locks.erase(*iter);
	  --client_held_lock_counts[old_lock_client];
	} else old_lock->length = removal_start - old_lock->start;
      } else {
	if (old_lock_end  > removal_end) {
	  ceph_filelock append_lock = *old_lock;
	  append_lock.start = removal_end + 1;
	  append_lock.length = old_lock_end - append_lock.start + 1;
	  held_locks.insert(pair<uint64_t, ceph_filelock>
			    (append_lock.start, append_lock));
	  ++client_held_lock_counts[(client_t)old_lock->client];
	}
	if (old_lock->start < removal_start) {
	  old_lock->length = removal_start - old_lock->start;
	} else {
	  dout(15) << "erasing " << (*iter)->second << dendl;
	  held_locks.erase(*iter);
	  --client_held_lock_counts[old_lock_client];
	}
      }
      if (!client_held_lock_counts[old_lock_client]) {
	client_held_lock_counts.erase(old_lock_client);
      }
    }

    /* okay, we've removed the locks, but removing them might allow some
     * other waiting locks to come through */
    if (get_waiting_overlaps(removal_lock, crossed_waiting_locks)) {
      /*let's do this the SUPER lazy way for now. Should work out something
	that's slightly less slow and wasteful, though.
	1) Remove lock from waiting_locks.
	2) attempt to insert lock via add_lock
	3) Add to success list if we get back "true"

	In the future, should probably set this up to detect some
	guaranteed blocks and do fewer map lookups.
       */
      for (list<multimap<uint64_t, ceph_filelock>::iterator>::iterator
	     iter = crossed_waiting_locks.begin();
	   iter != crossed_waiting_locks.end();
	   ++iter) {
	ceph_filelock cur_lock = (*iter)->second;
	waiting_locks.erase(*iter);
	--client_waiting_lock_counts[(client_t)cur_lock.client];
	if (!client_waiting_lock_counts[(client_t)cur_lock.client]) {
	  client_waiting_lock_counts.erase((client_t)cur_lock.client);
	}
	if(add_lock(cur_lock, true)) activated_locks.push_back(cur_lock);
      }
    }
  }

  bool remove_all_from (client_t client) {
    bool cleared_any = false;
    if (client_held_lock_counts.count(client)) {
      remove_all_from(client, held_locks);
      client_held_lock_counts.erase(client);
      cleared_any = true;
    }
    if (client_waiting_lock_counts.count(client)) {
      remove_all_from(client, waiting_locks);
      client_waiting_lock_counts.erase(client);
    }
    return cleared_any;
  }

private:
  /**
   * Adjust old locks owned by a single process so that process can set
   * a new lock of different type. Handle any changes needed to the old locks
   * (and the new lock) so that once the new lock is inserted into the 
   * held_locks list the process has a coherent, non-fragmented set of lock
   * ranges. Make sure any overlapping locks are combined, trimmed, and removed
   * as needed.
   * This function should only be called once you know the lock will be
   * inserted, as it DOES adjust new_lock. You can call this function
   * on an empty list, in which case it does nothing.
   * This function does not remove elements from the list, so regard the list
   * as bad information following function invocation.
   *
   * new_lock: The new lock the process has requested.
   * old_locks: list of all locks currently held by same
   *    client/process that overlap new_lock.
   * neighbor_locks: locks owned by same process that neighbor new_lock on
   *    left or right side.
   */
  void adjust_locks(list<multimap<uint64_t, ceph_filelock>::iterator> old_locks,
		    ceph_filelock& new_lock,
		    list<multimap<uint64_t, ceph_filelock>::iterator>
		    neighbor_locks) {
    dout(15) << "adjust_locks" << dendl;
    bool new_lock_to_end = (0 == new_lock.length);
    bool old_lock_to_end;
    uint64_t new_lock_start = new_lock.start;
    uint64_t new_lock_end = new_lock.start + new_lock.length - 1;
    uint64_t old_lock_start, old_lock_end;
    __s64 old_lock_client = 0;
    ceph_filelock *old_lock;
    for (list<multimap<uint64_t, ceph_filelock>::iterator>::iterator
	   iter = old_locks.begin();
	 iter != old_locks.end();
	 ++iter) {
      old_lock = &(*iter)->second;
      dout(15) << "adjusting lock: " << *old_lock << dendl;
      old_lock_to_end = (0 == old_lock->length);
      old_lock_start = old_lock->start;
      old_lock_end = old_lock->start + old_lock->length - 1;
      new_lock_start = new_lock.start;
      new_lock_end = new_lock.start + new_lock.length - 1;
      old_lock_client = old_lock->client;
      if (new_lock_to_end || old_lock_to_end) {
	//special code path to deal with a length set at 0
	dout(15) << "one lock extends forever" << dendl;
	if (old_lock->type == new_lock.type) {
	  //just unify them in new lock, remove old lock
	  dout(15) << "same lock type, unifying" << dendl;
	  new_lock.start = (new_lock_start < old_lock_start) ? new_lock_start :
	    old_lock_start;
	  new_lock.length = 0;
	  held_locks.erase(*iter);
	  --client_held_lock_counts[old_lock_client];
	} else { //not same type, have to keep any remains of old lock around
	  dout(15) << "shrinking old lock" << dendl;
	  if (new_lock_to_end) {
	    if (old_lock_start < new_lock_start) {
	      old_lock->length = new_lock_start - old_lock_start;
	    } else {
	      held_locks.erase(*iter);
	      --client_held_lock_counts[old_lock_client];
	    }
	  } else { //old lock extends past end of new lock
	    ceph_filelock appended_lock = *old_lock;
	    appended_lock.start = new_lock_end + 1;
	    held_locks.insert(pair<uint64_t, ceph_filelock>
			      (appended_lock.start, appended_lock));
	    ++client_held_lock_counts[(client_t)old_lock->client];
	    if (old_lock_start < new_lock_start) {
	      old_lock->length = new_lock_start - old_lock_start;
	    } else {
	      held_locks.erase(*iter);
	      --client_held_lock_counts[old_lock_client];
	    }
	  }
	}
      } else {
	if (old_lock->type == new_lock.type) { //just merge them!
	  dout(15) << "merging locks, they're the same type" << dendl;
	  new_lock.start = (old_lock_start < new_lock_start ) ? old_lock_start :
	    new_lock_start;
	  int new_end = (new_lock_end > old_lock_end) ? new_lock_end :
	    old_lock_end;
	  new_lock.length = new_end - new_lock.start + 1;
	  dout(15) << "erasing lock " << (*iter)->second << dendl;
	  held_locks.erase(*iter);
	  --client_held_lock_counts[old_lock_client];
	} else { //we'll have to update sizes and maybe make new locks
	  dout(15) << "locks aren't same type, changing sizes" << dendl;
	  if (old_lock_end > new_lock_end) { //add extra lock after new_lock
	    ceph_filelock appended_lock = *old_lock;
	    appended_lock.start = new_lock_end + 1;
	    appended_lock.length = old_lock_end - appended_lock.start + 1;
	    held_locks.insert(pair<uint64_t, ceph_filelock>
			      (appended_lock.start, appended_lock));
	    ++client_held_lock_counts[(client_t)old_lock->client];
	  }
	  if (old_lock_start < new_lock_start) {
	    old_lock->length = new_lock_start - old_lock_start;
	  } else { //old_lock starts inside new_lock, so remove it
	    //if it extended past new_lock_end it's been replaced
	    held_locks.erase(*iter);
	    --client_held_lock_counts[old_lock_client];
	  }
	}
      }
      if (!client_held_lock_counts[old_lock_client]) {
	client_held_lock_counts.erase(old_lock_client);
      }
    }

    //make sure to coalesce neighboring locks
    for (list<multimap<uint64_t, ceph_filelock>::iterator>::iterator
	   iter = neighbor_locks.begin();
	 iter != neighbor_locks.end();
	 ++iter) {
      old_lock = &(*iter)->second;
      old_lock_client = old_lock->client;
      dout(15) << "lock to coalesce: " << *old_lock << dendl;
      /* because if it's a neibhoring lock there can't be any self-overlapping
	 locks that covered it */
      if (old_lock->type == new_lock.type) { //merge them
	if (0 == new_lock.length) {
	  if (old_lock->start + old_lock->length == new_lock.start) {
	    new_lock.start = old_lock->start;
	  } else assert(0); /* if there's no end to new_lock, the neighbor
			       HAS TO be to left side */
	} else if (0 == old_lock->length) {
	  if (new_lock.start + new_lock.length == old_lock->start) {
	    new_lock.length = 0;
	  } else assert(0); //same as before, but reversed
	} else {
	  if (old_lock->start + old_lock->length == new_lock.start) {
	    new_lock.start = old_lock->start;
	    new_lock.length = old_lock->length + new_lock.length;
	  } else if (new_lock.start + new_lock.length == old_lock->start) {
	    new_lock.length = old_lock->length + new_lock.length;
	  }
	}
	held_locks.erase(*iter);
	--client_held_lock_counts[old_lock_client];
      }
      if (!client_held_lock_counts[old_lock_client]) {
	client_held_lock_counts.erase(old_lock_client);
      }
    }
  }

  //this won't reset the counter map value, do that yourself
  void remove_all_from(client_t client, multimap<uint64_t, ceph_filelock>& locks) {
    multimap<uint64_t, ceph_filelock>::iterator iter = locks.begin();
    while (iter != locks.end()) {
      if ((client_t)iter->second.client == client) {
	locks.erase(iter++);
      } else ++iter;
    }
  }

  //get last lock prior to start position
  multimap<uint64_t, ceph_filelock>::iterator
  get_lower_bound(uint64_t start, multimap<uint64_t, ceph_filelock>& lock_map) {
    multimap<uint64_t, ceph_filelock>::iterator lower_bound =
      lock_map.lower_bound(start);
    if ((lower_bound->first != start)
	&& (start != 0)
	&& (lower_bound != lock_map.begin())) --lower_bound;
    if (lock_map.end() == lower_bound)
      dout(15) << "get_lower_dout(15)eturning end()" << dendl;
    else dout(15) << "get_lower_bound returning iterator pointing to "
		 << lower_bound->second << dendl;
    return lower_bound;
  }

  //get latest-starting lock that goes over the byte "end"
  multimap<uint64_t, ceph_filelock>::iterator
  get_last_before(uint64_t end, multimap<uint64_t, ceph_filelock>& lock_map) {
    multimap<uint64_t, ceph_filelock>::iterator last =
      lock_map.upper_bound(end);
    if (last != lock_map.begin()) --last;
    if (lock_map.end() == last)
      dout(15) << "get_last_before returning end()" << dendl;
    else dout(15) << "get_last_before returning iterator pointing to "
		 << last->second << dendl;
    return last;
  }

  /*
   * See if an iterator's lock covers any of the same bounds as a given range
   * Rules: locks cover "length" bytes from "start", so the last covered
   * byte is at start + length - 1.
   * If the length is 0, the lock covers from "start" to the end of the file.
   */
  bool share_space(multimap<uint64_t, ceph_filelock>::iterator& iter,
		   uint64_t start, uint64_t end) {
    bool ret = ((iter->first >= start && iter->first <= end) ||
		((iter->first < start) &&
		 (((iter->first + iter->second.length - 1) >= start) ||
		  (0 == iter->second.length))));
    dout(15) << "share_space got start: " << start << ", end: " << end
	    << ", lock: " << iter->second << ", returning " << ret << dendl;
    return ret;
  }
  bool share_space(multimap<uint64_t, ceph_filelock>::iterator& iter,
		   ceph_filelock& lock) {
    return share_space(iter, lock.start, lock.start+lock.length-1);
  }
  
  /*
   *get a list of all locks overlapping with the given lock's range
   * lock: the lock to compare with.
   * overlaps: an empty list, to be filled.
   * Returns: true if at least one lock overlaps.
   */
  bool get_overlapping_locks(ceph_filelock& lock,
			     list<multimap<uint64_t, ceph_filelock>::iterator> & overlaps,
			     list<multimap<uint64_t, ceph_filelock>::iterator> *self_neighbors) {
    dout(15) << "get_overlapping_locks" << dendl;
    // create a lock starting one earlier and ending one later
    // to check for neighbors
    ceph_filelock neighbor_check_lock = lock;
    neighbor_check_lock.start = neighbor_check_lock.start - 1;
    if (neighbor_check_lock.length)
      neighbor_check_lock.length = neighbor_check_lock.length+ 2;
    //find the last held lock starting at the point after lock
    multimap<uint64_t, ceph_filelock>::iterator iter =
      get_last_before(lock.start + lock.length, held_locks);
    bool cont = iter != held_locks.end();
    while(cont) {
      if (share_space(iter, lock)) {
	overlaps.push_front(iter);
      } else if (self_neighbors &&
		 (neighbor_check_lock.client == iter->second.client) &&
		 (neighbor_check_lock.pid == iter->second.pid) &&
		 share_space(iter, neighbor_check_lock)) {
	self_neighbors->push_front(iter);
      }
      if ((iter->first < lock.start) && (CEPH_LOCK_EXCL == iter->second.type)) {
	//can't be any more overlapping locks or they'd interfere with this one
	cont = false;
      } else if (held_locks.begin() == iter) cont = false;
      else --iter;
    }
    return !overlaps.empty();
  }
  
  bool get_overlapping_locks(ceph_filelock& lock,
			     list<multimap<uint64_t, ceph_filelock>::iterator>& overlaps) {
    return get_overlapping_locks(lock, overlaps, NULL);
  }

  /**
   * Get a list of all waiting locks that overlap with the given lock's range.
   * lock: specifies the range to compare with
   * overlaps: an empty list, to be filled
   * Returns: true if at least one waiting_lock overlaps
   */
  bool get_waiting_overlaps(ceph_filelock& lock,
			    list<multimap<uint64_t, ceph_filelock>::iterator>&
			    overlaps) {
    dout(15) << "get_waiting_overlaps" << dendl;
    multimap<uint64_t, ceph_filelock>::iterator iter =
      get_last_before(lock.start + lock.length - 1, waiting_locks);
    bool cont = iter != waiting_locks.end();
    while(cont) {
      if (share_space(iter, lock)) overlaps.push_front(iter);
      if (waiting_locks.begin() == iter) cont = false;
      --iter;
    }
    return !overlaps.empty();
  }

  /*
   * split a list of locks up by whether they're owned by same
   * process as given lock
   * owner: the owning lock
   * locks: the list of locks (obtained from get_overlapping_locks, probably)
   *        Will have all locks owned by owner removed
   * owned_locks: an empty list, to be filled with the locks owned by owner
   */
  void split_by_owner(ceph_filelock& owner,
		      list<multimap<uint64_t, ceph_filelock>::iterator> & locks,
		      list<multimap<uint64_t, ceph_filelock>::iterator> & owned_locks) {
    list<multimap<uint64_t, ceph_filelock>::iterator>::iterator
      iter = locks.begin();
    dout(15) << "owner lock: " << owner << dendl;
    while (iter != locks.end()) {
      dout(15) << "comparing to " << (*iter)->second << dendl;
      if ((*iter)->second.client == owner.client &&
	  (*iter)->second.pid_namespace == owner.pid_namespace &&
	  (*iter)->second.pid == owner.pid) {
	dout(15) << "success, pushing to owned_locks" << dendl;
	owned_locks.push_back(*iter);
	iter = locks.erase(iter);
      } else {
	dout(15) << "failure, something not equal in this group "
		<< (*iter)->second.client << ":" << owner.client << ","
		<< (*iter)->second.pid_namespace << ":" << owner.pid_namespace
		<< "," << (*iter)->second.pid << ":" << owner.pid << dendl;
	++iter;
      }
    }
  }

  ceph_filelock *contains_exclusive_lock(list<multimap<uint64_t, ceph_filelock>::iterator>& locks) {
    for (list<multimap<uint64_t, ceph_filelock>::iterator>::iterator
	   iter = locks.begin();
	 iter != locks.end();
	 ++iter) {
      if (CEPH_LOCK_EXCL == (*iter)->second.type) return &(*iter)->second;
    }
    return NULL;
  }

public:
  void encode(bufferlist& bl) const {
    ::encode(held_locks, bl);
    ::encode(waiting_locks, bl);
  }
  void decode(bufferlist::iterator& bl) {
    ::decode(held_locks, bl);
    ::decode(waiting_locks, bl);
  }
};
WRITE_CLASS_ENCODER(ceph_lock_state_t)

#endif
