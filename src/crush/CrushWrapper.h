// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_CRUSH_WRAPPER_H
#define CEPH_CRUSH_WRAPPER_H

#define BUG_ON(x) assert(!(x))
#include "include/types.h"

extern "C" {
#include "crush.h"
#include "hash.h"
#include "mapper.h"
#include "builder.h"
}

#include "include/err.h"
#include "include/encoding.h"

#include <stdlib.h>
#include <map>
#include <set>
#include <string>

#include <iostream> //for testing, remove

WRITE_RAW_ENCODER(crush_rule_mask)   // it's all u8's

inline static void encode(const crush_rule_step &s, bufferlist &bl)
{
  ::encode(s.op, bl);
  ::encode(s.arg1, bl);
  ::encode(s.arg2, bl);
}
inline static void decode(crush_rule_step &s, bufferlist::iterator &p)
{
  ::decode(s.op, p);
  ::decode(s.arg1, p);
  ::decode(s.arg2, p);
}



using namespace std;
class CrushWrapper {
public:
  struct crush_map *crush;
  std::map<int, string> type_map; /* bucket/device type names */
  std::map<int, string> name_map; /* bucket/device names */
  std::map<int, string> rule_name_map;

  /* reverse maps */
  bool have_rmaps;
  std::map<string, int> type_rmap, name_rmap, rule_name_rmap;

private:
  void build_rmaps() {
    if (have_rmaps) return;
    build_rmap(type_map, type_rmap);
    build_rmap(name_map, name_rmap);
    build_rmap(rule_name_map, rule_name_rmap);
    have_rmaps = true;
  }
  void build_rmap(const map<int, string> &f, std::map<string, int> &r) {
    r.clear();
    for (std::map<int, string>::const_iterator p = f.begin(); p != f.end(); ++p)
      r[p->second] = p->first;
  }

public:
  CrushWrapper() : crush(0), have_rmaps(false) {}
  ~CrushWrapper() {
    if (crush) crush_destroy(crush);
  }

  /* building */
  void create() {
    if (crush) crush_destroy(crush);
    crush = crush_create();
  }

  // bucket types
  int get_num_type_names() {
    return type_map.size();
  }
  int get_type_id(const char *s) {
    string name(s);
    build_rmaps();
    if (type_rmap.count(name))
      return type_rmap[name];
    return 0;
  }
  const char *get_type_name(int t) {
    if (type_map.count(t))
      return type_map[t].c_str();
    return 0;
  }
  void set_type_name(int i, const char *n) {
    string name(n);
    type_map[i] = name;
    if (have_rmaps)
      type_rmap[name] = i;
  }

  // item/bucket names
  int get_item_id(const char *s) {
    string name(s);
    build_rmaps();
    if (name_rmap.count(name))
      return name_rmap[name];
    return 0;  /* hrm */
  }
  const char *get_item_name(int t) {
    if (name_map.count(t))
      return name_map[t].c_str();
    return 0;
  }
  void set_item_name(int i, const char *n) {
    string name(n);
    name_map[i] = name;
    if (have_rmaps)
      name_rmap[name] = i;
  }

  // rule names
  int get_rule_id(const char *n) {
    string name(n);
    build_rmaps();
    if (rule_name_rmap.count(name))
      return rule_name_rmap[name];
    return 0;  /* hrm */
  }
  const char *get_rule_name(int t) {
    if (rule_name_map.count(t))
      return rule_name_map[t].c_str();
    return 0;
  }
  void set_rule_name(int i, const char *n) {
    string name(n);
    rule_name_map[i] = name;
    if (have_rmaps)
      rule_name_rmap[name] = i;
  }

  /*** devices ***/
  int get_max_devices() {
    if (!crush) return 0;
    return crush->max_devices;
  }


  /*** rules ***/
private:
  crush_rule *get_rule(unsigned ruleno) {
    if (!crush) return (crush_rule *)(-ENOENT);
    if (ruleno >= crush->max_rules)
      return 0;
    return crush->rules[ruleno];
  }
  crush_rule_step *get_rule_step(unsigned ruleno, unsigned step) {
    crush_rule *n = get_rule(ruleno);
    if (!n) return (crush_rule_step *)(-EINVAL);
    if (step >= n->len) return (crush_rule_step *)(-EINVAL);
    return &n->steps[step];
  }

public:
  /* accessors */
  int get_max_rules() {
    if (!crush) return 0;
    return crush->max_rules;
  }
  bool rule_exists(unsigned ruleno) {
    if (!crush) return false;
    if (ruleno < crush->max_rules &&
	crush->rules[ruleno] != NULL)
      return true;
    return false;
  }
  int get_rule_len(unsigned ruleno) {
    crush_rule *r = get_rule(ruleno);
    if (IS_ERR(r)) return PTR_ERR(r);
    return r->len;
  }
  int get_rule_mask_ruleset(unsigned ruleno) {
    crush_rule *r = get_rule(ruleno);
    if (IS_ERR(r)) return -1;
    return r->mask.ruleset;
  }
  int get_rule_mask_type(unsigned ruleno) {
    crush_rule *r = get_rule(ruleno);
    if (IS_ERR(r)) return -1;
    return r->mask.type;
  }
  int get_rule_mask_min_size(unsigned ruleno) {
    crush_rule *r = get_rule(ruleno);
    if (IS_ERR(r)) return -1;
    return r->mask.min_size;
  }
  int get_rule_mask_max_size(unsigned ruleno) {
    crush_rule *r = get_rule(ruleno);
    if (IS_ERR(r)) return -1;
    return r->mask.max_size;
  }
  int get_rule_op(unsigned ruleno, unsigned step) {
    crush_rule_step *s = get_rule_step(ruleno, step);
    if (IS_ERR(s)) return PTR_ERR(s);
    return s->op;
  }
  int get_rule_arg1(unsigned ruleno, unsigned step) {
    crush_rule_step *s = get_rule_step(ruleno, step);
    if (IS_ERR(s)) return PTR_ERR(s);
    return s->arg1;
  }
  int get_rule_arg2(unsigned ruleno, unsigned step) {
    crush_rule_step *s = get_rule_step(ruleno, step);
    if (IS_ERR(s)) return PTR_ERR(s);
    return s->arg2;
  }

  /* modifiers */
  int add_rule(int len, int pool, int type, int minsize, int maxsize, int ruleno) {
    if (!crush) return -ENOENT;
    crush_rule *n = crush_make_rule(len, pool, type, minsize, maxsize);
    ruleno = crush_add_rule(crush, n, ruleno);
    return ruleno;
  }
  int set_rule_step(unsigned ruleno, unsigned step, int op, int arg1, int arg2) {
    if (!crush) return -ENOENT;
    crush_rule *n = get_rule(ruleno);
    if (!n) return -1;
    crush_rule_set_step(n, step, op, arg1, arg2);
    return 0;
  }
  int set_rule_step_take(unsigned ruleno, unsigned step, int val) {
    return set_rule_step(ruleno, step, CRUSH_RULE_TAKE, val, 0);
  }
  int set_rule_step_choose_firstn(unsigned ruleno, unsigned step, int val, int type) {
    return set_rule_step(ruleno, step, CRUSH_RULE_CHOOSE_FIRSTN, val, type);
  }
  int set_rule_step_choose_indep(unsigned ruleno, unsigned step, int val, int type) {
    return set_rule_step(ruleno, step, CRUSH_RULE_CHOOSE_INDEP, val, type);
  }
  int set_rule_step_choose_leaf_firstn(unsigned ruleno, unsigned step, int val, int type) {
    return set_rule_step(ruleno, step, CRUSH_RULE_CHOOSE_LEAF_FIRSTN, val, type);
  }
  int set_rule_step_choose_leaf_indep(unsigned ruleno, unsigned step, int val, int type) {
    return set_rule_step(ruleno, step, CRUSH_RULE_CHOOSE_LEAF_INDEP, val, type);
  }
  int set_rule_step_emit(unsigned ruleno, unsigned step) {
    return set_rule_step(ruleno, step, CRUSH_RULE_EMIT, 0, 0);
  }



  /** buckets **/
private:
  crush_bucket *get_bucket(int id) {
    if (!crush)
      return (crush_bucket *)(-EINVAL);
    unsigned int pos = (unsigned int)(-1 - id);
    unsigned int max_buckets = crush->max_buckets;
    if (pos >= max_buckets)
      return (crush_bucket *)(-ENOENT);
    crush_bucket *ret = crush->buckets[pos];
    if (ret == NULL)
      return (crush_bucket *)(-ENOENT);
    return ret;
  }

public:
  int get_max_buckets() {
    if (!crush) return -EINVAL;
    return crush->max_buckets;
  }
  int get_next_bucket_id() {
    if (!crush) return -EINVAL;
    return crush_get_next_bucket_id(crush);
  }
  bool bucket_exists(int id) {
    crush_bucket *b = get_bucket(id);
    if (IS_ERR(b))
      return false;
    return true;
  }
  int get_bucket_weight(int id) {
    crush_bucket *b = get_bucket(id);
    if (IS_ERR(b)) return PTR_ERR(b);
    return b->weight;
  }
  int get_bucket_type(int id) {
    crush_bucket *b = get_bucket(id);
    if (IS_ERR(b)) return PTR_ERR(b);
    return b->type;
  }
  int get_bucket_alg(int id) {
    crush_bucket *b = get_bucket(id);
    if (IS_ERR(b)) return PTR_ERR(b);
    return b->alg;
  }
  int get_bucket_hash(int id) {
    crush_bucket *b = get_bucket(id);
    if (IS_ERR(b)) return PTR_ERR(b);
    return b->hash;
  }
  int get_bucket_size(int id) {
    crush_bucket *b = get_bucket(id);
    if (IS_ERR(b)) return PTR_ERR(b);
    return b->size;
  }
  int get_bucket_item(int id, int pos) {
    crush_bucket *b = get_bucket(id);
    if (IS_ERR(b)) return PTR_ERR(b);
    if ((__u32)pos >= b->size)
      return PTR_ERR(b);
    return b->items[pos];
  }
  int get_bucket_item_weight(int id, int pos) {
    crush_bucket *b = get_bucket(id);
    if (IS_ERR(b)) return PTR_ERR(b);
    return crush_get_bucket_item_weight(b, pos);
  }

  /* modifiers */
  int add_bucket(int bucketno, int alg, int hash, int type, int size,
		 int *items, int *weights) {
    crush_bucket *b = crush_make_bucket(alg, hash, type, size, items, weights);
    return crush_add_bucket(crush, bucketno, b);
  }

  void finalize() {
    assert(crush);
    crush_finalize(crush);
  }

  void set_max_devices(int m) {
    crush->max_devices = m;
  }

  int find_rule(int pool, int type, int size) {
    if (!crush) return -1;
    return crush_find_rule(crush, pool, type, size);
  }
  void do_rule(int rule, int x, vector<int>& out, int maxout, int forcefeed,
	       vector<__u32>& weight) {
    int rawout[maxout];
    int numrep = crush_do_rule(crush, rule, x, rawout, maxout,
			       forcefeed, &weight[0]);
    if (numrep < 0)
      numrep = 0;   // e.g., when forcefed device dne.
    out.resize(numrep);
    for (int i=0; i<numrep; i++)
      out[i] = rawout[i];
  }

  int read_from_file(const char *fn) {
    bufferlist bl;
    int r = bl.read_file(fn);
    if (r < 0) return r;
    bufferlist::iterator blp = bl.begin();
    decode(blp);
    return 0;
  }
  int write_to_file(const char *fn) {
    bufferlist bl;
    encode(bl);
    return bl.write_file(fn);
  }

  void encode(bufferlist &bl, bool lean=false) {
    if (!crush) create();  // duh.

    __u32 magic = CRUSH_MAGIC;
    ::encode(magic, bl);

    ::encode(crush->max_buckets, bl);
    ::encode(crush->max_rules, bl);
    ::encode(crush->max_devices, bl);

    // buckets
    for (int i=0; i<crush->max_buckets; i++) {
      __u32 alg = 0;
      if (crush->buckets[i]) alg = crush->buckets[i]->alg;
      ::encode(alg, bl);
      if (!alg) continue;

      ::encode(crush->buckets[i]->id, bl);
      ::encode(crush->buckets[i]->type, bl);
      ::encode(crush->buckets[i]->alg, bl);
      ::encode(crush->buckets[i]->hash, bl);
      ::encode(crush->buckets[i]->weight, bl);
      ::encode(crush->buckets[i]->size, bl);
      for (unsigned j=0; j<crush->buckets[i]->size; j++)
	::encode(crush->buckets[i]->items[j], bl);

      switch (crush->buckets[i]->alg) {
      case CRUSH_BUCKET_UNIFORM:
	::encode(((crush_bucket_uniform*)crush->buckets[i])->item_weight, bl);
	break;

      case CRUSH_BUCKET_LIST:
	for (unsigned j=0; j<crush->buckets[i]->size; j++) {
	  ::encode(((crush_bucket_list*)crush->buckets[i])->item_weights[j], bl);
	  ::encode(((crush_bucket_list*)crush->buckets[i])->sum_weights[j], bl);
	}
	break;

      case CRUSH_BUCKET_TREE:
	::encode(((crush_bucket_tree*)crush->buckets[i])->num_nodes, bl);
	for (unsigned j=0; j<((crush_bucket_tree*)crush->buckets[i])->num_nodes; j++)
	  ::encode(((crush_bucket_tree*)crush->buckets[i])->node_weights[j], bl);
	break;

      case CRUSH_BUCKET_STRAW:
	for (unsigned j=0; j<crush->buckets[i]->size; j++) {
	  ::encode(((crush_bucket_straw*)crush->buckets[i])->item_weights[j], bl);
	  ::encode(((crush_bucket_straw*)crush->buckets[i])->straws[j], bl);
	}
	break;
      }
    }

    // rules
    for (unsigned i=0; i<crush->max_rules; i++) {
      __u32 yes = crush->rules[i] ? 1:0;
      ::encode(yes, bl);
      if (!yes) continue;

      ::encode(crush->rules[i]->len, bl);
      ::encode(crush->rules[i]->mask, bl);
      for (unsigned j=0; j<crush->rules[i]->len; j++)
	::encode(crush->rules[i]->steps[j], bl);
    }

    // name info
    ::encode(type_map, bl);
    ::encode(name_map, bl);
    ::encode(rule_name_map, bl);
  }

  void decode(bufferlist::iterator &blp)
  {
    create();

    __u32 magic;
    ::decode(magic, blp);
    if (magic != CRUSH_MAGIC)
      throw buffer::malformed_input("bad magic number");

    ::decode(crush->max_buckets, blp);
    ::decode(crush->max_rules, blp);
    ::decode(crush->max_devices, blp);

    try {
      // buckets
      crush->buckets = (crush_bucket**)calloc(1, crush->max_buckets * sizeof(crush_bucket*));
      for (int i=0; i<crush->max_buckets; i++) {
	decode_crush_bucket(&crush->buckets[i], blp);
      }

      // rules
      crush->rules = (crush_rule**)calloc(1, crush->max_rules * sizeof(crush_rule*));
      for (unsigned i = 0; i < crush->max_rules; ++i) {
	__u32 yes;
	::decode(yes, blp);
	if (!yes) {
	  crush->rules[i] = NULL;
	  continue;
	}

	__u32 len;
	::decode(len, blp);
	crush->rules[i] = (crush_rule*)calloc(1, crush_rule_size(len));
	crush->rules[i]->len = len;
	::decode(crush->rules[i]->mask, blp);
	for (unsigned j=0; j<crush->rules[i]->len; j++)
	  ::decode(crush->rules[i]->steps[j], blp);
      }

      // name info
      ::decode(type_map, blp);
      ::decode(name_map, blp);
      ::decode(rule_name_map, blp);
      build_rmaps();

      finalize();
    }
    catch (...) {
      crush_destroy(crush);
      throw;
    }
  }

  void decode_crush_bucket(crush_bucket** bptr, bufferlist::iterator &blp)
  {
    __u32 alg;
    ::decode(alg, blp);
    if (!alg) {
      *bptr = NULL;
      return;
    }

    int size = 0;
    switch (alg) {
      case CRUSH_BUCKET_UNIFORM:
	size = sizeof(crush_bucket_uniform);
	break;
      case CRUSH_BUCKET_LIST:
	size = sizeof(crush_bucket_list);
	break;
      case CRUSH_BUCKET_TREE:
	size = sizeof(crush_bucket_tree);
	break;
      case CRUSH_BUCKET_STRAW:
	size = sizeof(crush_bucket_straw);
	break;
      default: {
	char str[128];
	snprintf(str, sizeof(str), "unsupported bucket algorithm: %d", alg);
	throw buffer::malformed_input(str);
      }
    }
    crush_bucket *bucket = (crush_bucket*)calloc(1, size);
    *bptr = bucket;

    ::decode(bucket->id, blp);
    ::decode(bucket->type, blp);
    ::decode(bucket->alg, blp);
    ::decode(bucket->hash, blp);
    ::decode(bucket->weight, blp);
    ::decode(bucket->size, blp);

    bucket->items = (__s32*)calloc(1, bucket->size * sizeof(__s32));
    for (unsigned j = 0; j < bucket->size; ++j) {
      ::decode(bucket->items[j], blp);
    }

    bucket->perm = (__u32*)calloc(1, bucket->size * sizeof(__s32));
    bucket->perm_n = 0;

    switch (bucket->alg) {
      case CRUSH_BUCKET_UNIFORM: {
	::decode(((crush_bucket_uniform*)bucket)->item_weight, blp);
	break;
      }

      case CRUSH_BUCKET_LIST: {
	crush_bucket_list* cbl = (crush_bucket_list*)bucket;
	cbl->item_weights = (__u32*)calloc(1, bucket->size * sizeof(__u32));
	cbl->sum_weights = (__u32*)calloc(1, bucket->size * sizeof(__u32));

	for (unsigned j = 0; j < bucket->size; ++j) {
	  ::decode(cbl->item_weights[j], blp);
	  ::decode(cbl->sum_weights[j], blp);
	}
	break;
      }

      case CRUSH_BUCKET_TREE: {
	unsigned num_nodes;
	crush_bucket_tree* cbt = (crush_bucket_tree*)bucket;
	::decode(num_nodes, blp);
	cbt->num_nodes = num_nodes;
	cbt->node_weights = (__u32*)calloc(1, num_nodes * sizeof(__u32));
	for (unsigned j=0; j<num_nodes; j++) {
	  ::decode(cbt->node_weights[j], blp);
	}
	break;
      }

      case CRUSH_BUCKET_STRAW: {
	crush_bucket_straw* cbs = (crush_bucket_straw*)bucket;
	cbs->straws = (__u32*)calloc(1, bucket->size * sizeof(__u32));
	cbs->item_weights = (__u32*)calloc(1, bucket->size * sizeof(__u32));
	for (unsigned j = 0; j < bucket->size; ++j) {
	  ::decode(cbs->item_weights[j], blp);
	  ::decode(cbs->straws[j], blp);
	}
	break;
      }

      default:
	// We should have handled this case in the first switch statement
	assert(0);
	break;
    }
  }
};

#endif
