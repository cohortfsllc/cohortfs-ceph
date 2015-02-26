// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
#include "include/interval_set.h"
#include "include/buffer.h"
#include "include/encoding.h"
#include <list>
#include <map>
#include <set>

#ifndef OBJECT_H
#define OBJECT_H

class ContDesc {
public:
  int objnum;
  unsigned seqnum;
  std::string prefix;
  std::string oid;

  ContDesc() :
    objnum(0), seqnum(0), prefix("") {}

  ContDesc(int objnum,
	   unsigned seqnum,
	   const std::string &prefix) :
    objnum(objnum), seqnum(seqnum), prefix(prefix) {}

  bool operator==(const ContDesc &rhs) {
    return (rhs.objnum == objnum &&
	    rhs.seqnum == seqnum &&
	    rhs.prefix == prefix &&
	    rhs.oid == oid);
  }

  bool operator<(const ContDesc &rhs) const {
    return seqnum < rhs.seqnum;
  }

  bool operator!=(const ContDesc &rhs) {
    return !((*this) == rhs);
  }
  void encode(bufferlist &bl) const;
  void decode(bufferlist::iterator &bp);
};
WRITE_CLASS_ENCODER(ContDesc)

std::ostream &operator<<(std::ostream &out, const ContDesc &rhs);

class ContentsGenerator {
public:

  class iterator_impl {
  public:
    virtual char operator*() = 0;
    virtual iterator_impl &operator++() = 0;
    virtual void seek(uint64_t pos) = 0;
    virtual bool end() = 0;
    virtual ContDesc get_cont() const = 0;
    virtual uint64_t get_pos() const = 0;
    virtual ~iterator_impl() {};
  };

  class iterator {
  public:
    ContentsGenerator *parent;
    iterator_impl *impl;
    char operator *() { return **impl; }
    iterator &operator++() { ++(*impl); return *this; };
    void seek(uint64_t pos) { impl->seek(pos); }
    bool end() { return impl->end(); }
    ~iterator() { parent->put_iterator_impl(impl); }
    iterator(const iterator &rhs) : parent(rhs.parent) {
      impl = parent->dup_iterator_impl(rhs.impl);
    }
    iterator &operator=(const iterator &rhs) {
      iterator new_iter(rhs);
      swap(new_iter);
      return *this;
    }
    void swap(iterator &other) {
      ContentsGenerator *otherparent = other.parent;
      other.parent = parent;
      parent = otherparent;

      iterator_impl *otherimpl = other.impl;
      other.impl = impl;
      impl = otherimpl;
    }
    iterator(ContentsGenerator *parent, iterator_impl *impl) :
      parent(parent), impl(impl) {}
  };

  virtual uint64_t get_length(const ContDesc &in) = 0;

  virtual void get_ranges_map(
    const ContDesc &cont, std::map<uint64_t, uint64_t> &out) = 0;
  void get_ranges(const ContDesc &cont, interval_set<uint64_t> &out) {
    std::map<uint64_t, uint64_t> ranges;
    get_ranges_map(cont, ranges);
    for (auto i = ranges.begin();
	 i != ranges.end();
	 ++i) {
      out.insert(i->first, i->second);
    }
  }


  virtual iterator_impl *get_iterator_impl(const ContDesc &in) = 0;

  virtual iterator_impl *dup_iterator_impl(const iterator_impl *in) = 0;

  virtual void put_iterator_impl(iterator_impl *in) = 0;

  virtual ~ContentsGenerator() {};

  iterator get_iterator(const ContDesc &in) {
    return iterator(this, get_iterator_impl(in));
  }
};

class RandGenerator : public ContentsGenerator {
public:
  class RandWrap {
  public:
    unsigned int state;
    RandWrap(unsigned int seed)
    {
      state = seed;
    }

    int operator()()
    {
      return rand_r(&state);
    }
  };

  class iterator_impl : public ContentsGenerator::iterator_impl {
  public:
    uint64_t pos;
    ContDesc cont;
    RandWrap rand;
    RandGenerator *cont_gen;
    char current;
    iterator_impl(const ContDesc &cont, RandGenerator *cont_gen) :
      pos(0), cont(cont), rand(cont.seqnum), cont_gen(cont_gen) {
      current = rand();
    }

    ContDesc get_cont() const { return cont; }
    uint64_t get_pos() const { return pos; }

    iterator_impl &operator++() {
      pos++;
      current = rand();
      return *this;
    }

    char operator*() {
      return current;
    }

    void seek(uint64_t _pos) {
      if (_pos < pos) {
	iterator_impl begin = iterator_impl(cont, cont_gen);
	begin.seek(_pos);
	*this = begin;
      }
      while (pos < _pos) {
	++(*this);
      }
    }

    bool end() {
      return pos >= cont_gen->get_length(cont);
    }
  };

  ContentsGenerator::iterator_impl *get_iterator_impl(const ContDesc &in) {
    RandGenerator::iterator_impl *i = new iterator_impl(in, this);
    return i;
  }

  void put_iterator_impl(ContentsGenerator::iterator_impl *in) {
    delete in;
  }

  ContentsGenerator::iterator_impl *dup_iterator_impl(
    const ContentsGenerator::iterator_impl *in) {
    ContentsGenerator::iterator_impl *retval = get_iterator_impl(in->get_cont());
    retval->seek(in->get_pos());
    return retval;
  }
};

class VarLenGenerator : public RandGenerator {
  uint64_t max_length;
  uint64_t min_stride_size;
  uint64_t max_stride_size;
public:
  VarLenGenerator(
    uint64_t length, uint64_t min_stride_size, uint64_t max_stride_size) :
    max_length(length),
    min_stride_size(min_stride_size),
    max_stride_size(max_stride_size) {}
  void get_ranges_map(
    const ContDesc &cont, std::map<uint64_t, uint64_t> &out);
  uint64_t get_length(const ContDesc &in) {
    RandWrap rand(in.seqnum);
    return (rand() % max_length);
  }
};

class AttrGenerator : public RandGenerator {
  uint64_t max_len;
public:
  AttrGenerator(uint64_t max_len) : max_len(max_len) {}
  void get_ranges_map(
    const ContDesc &cont, std::map<uint64_t, uint64_t> &out) {
    out.insert(std::make_pair(0, get_length(cont)));
  }
  uint64_t get_length(const ContDesc &in) {
    RandWrap rand(in.seqnum);
    return (rand() % max_len);
  }
  bufferlist gen_bl(const ContDesc &in) {
    bufferlist bl;
    for (iterator i = get_iterator(in); !i.end(); ++i) {
      bl.append(*i);
    }
    assert(bl.length() < max_len);
    return bl;
  }
};

class AppendGenerator : public RandGenerator {
  uint64_t off;
  uint64_t min_append_size;
  uint64_t max_append_size;
  uint64_t max_append_total;

  uint64_t round_up(uint64_t in, uint64_t by) {
    if (by)
      in += (by - (in % by));
    return in;
  }

public:
  AppendGenerator(
    uint64_t off,
    uint64_t min_append_size,
    uint64_t _max_append_size,
    uint64_t max_append_multiple) :
    off(off),
    min_append_size(min_append_size),
    max_append_size(_max_append_size) {
    max_append_total = max_append_multiple * max_append_size;
  }
  uint64_t get_append_size(const ContDesc &in) {
    RandWrap rand(in.seqnum);
    return rand() % max_append_total;
  }
  uint64_t get_length(const ContDesc &in) {
    return off + get_append_size(in);
  }
  void get_ranges_map(
    const ContDesc &cont, std::map<uint64_t, uint64_t> &out);
};

class ObjectDesc {
public:
  ObjectDesc()
    : exists(false), dirty(false),
      version(0) {}
  ObjectDesc(const ContDesc &init, std::shared_ptr<ContentsGenerator> cont_gen)
    : exists(false), dirty(false),
      version(0) {
    layers.push_front(make_pair(cont_gen, init));
  }

  class iterator {
  public:
    uint64_t pos;
    ObjectDesc &oid;
    std::list<std::pair<std::list<std::pair<std::shared_ptr<ContentsGenerator>,
			     ContDesc> >::iterator,
		   uint64_t> > stack;
    std::map<ContDesc,ContentsGenerator::iterator> cont_iters;
    uint64_t limit;
    std::list<std::pair<std::shared_ptr<ContentsGenerator>,
	      ContDesc> >::iterator cur_cont;

    iterator(ObjectDesc &oid) :
      pos(0), oid(oid) {
      limit = oid.layers.begin()->first->get_length(oid.layers.begin()->second);
      cur_cont = oid.layers.begin();
      advance(true);
    }

    iterator &advance(bool init);
    iterator &operator++() {
      return advance(false);
    }

    char operator*() {
      if (cur_cont == oid.layers.end()) {
	return '\0';
      } else {
	auto j = cont_iters.find(cur_cont->second);
	assert(j != cont_iters.end());
	return *(j->second);
      }
    }

    bool end() {
      return pos >= oid.layers.begin()->first->get_length(
	oid.layers.begin()->second);
    }

    void seek(uint64_t _pos) {
      if (_pos < pos) {
	assert(0);
      }
      while (pos < _pos) {
	++(*this);
      }
    }
  };

  iterator begin() {
    return iterator(*this);
  }

  bool deleted() {
    return !exists;
  }

  bool has_contents() {
    return layers.size();
  }

  // takes ownership of gen
  void update(std::shared_ptr<ContentsGenerator> gen, const ContDesc &next);
  bool check(bufferlist &to_check);
  const ContDesc &most_recent();
  std::shared_ptr<ContentsGenerator> most_recent_gen() {
    return layers.begin()->first;
  }
  std::map<std::string, ContDesc> attrs; // Both omap and xattrs
  bufferlist header;
  bool exists;
  bool dirty;

  uint64_t version;
private:
  std::list<std::pair<std::shared_ptr<ContentsGenerator>, ContDesc> > layers;
};

#endif
