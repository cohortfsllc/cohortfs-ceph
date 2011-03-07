// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#include "common/config.h"
#include "common/errno.h"
#include "common/safe_io.h"
#include "FileJournal.h"
#include "include/color.h"

#include <fcntl.h>
#include <sstream>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mount.h>


#define DOUT_SUBSYS journal
#undef dout_prefix
#define dout_prefix *_dout << "journal "

const static int64_t ONE_MEG(1 << 20);

int FileJournal::_open(bool forwrite, bool create)
{
  int flags, ret;

  if (forwrite) {
    flags = O_RDWR;
    if (directio)
      flags |= O_DIRECT | O_SYNC;
  } else {
    flags = O_RDONLY;
  }
  if (create)
    flags |= O_CREAT;
  
  if (fd >= 0) {
    if (TEMP_FAILURE_RETRY(::close(fd))) {
      int err = errno;
      derr << "FileJournal::_open: error closing old fd: "
	   << cpp_strerror(err) << dendl;
    }
  }
  fd = TEMP_FAILURE_RETRY(::open(fn.c_str(), flags, 0644));
  if (fd < 0) {
    int err = errno;
    derr << "FileJournal::_open : unable to open journal: open() "
	 << "failed: " << cpp_strerror(err) << dendl;
    return -err;
  }

  struct stat st;
  ret = ::fstat(fd, &st);
  if (ret) {
    int err = errno;
    derr << "FileJournal::_open: unable to fstat journal: "
         << cpp_strerror(err) << dendl;
    return -err;
  }

  if (S_ISBLK(st.st_mode))
    ret = _open_block_device();
  else
    ret = _open_file(st.st_size, st.st_blksize, create);

  if (ret)
    return ret;

  /* We really want max_size to be a multiple of block_size. */
  max_size -= max_size % block_size;

  // static zeroed buffer for alignment padding
  delete [] zero_buf;
  zero_buf = new char[header.alignment];
  memset(zero_buf, 0, header.alignment);

  dout(1) << "_open " << fn << " fd " << fd
	  << ": " << max_size 
	  << " bytes, block size " << block_size
	  << " bytes, directio = " << directio << dendl;
  return 0;
}

int FileJournal::_open_block_device()
{
  int ret = 0;
  int64_t bdev_sz = 0;
#ifdef BLKGETSIZE64
  // ioctl block device
  ret = ::ioctl(fd, BLKGETSIZE64, &bdev_sz);
#elif BLKGETSIZE
  // hrm, try the 32 bit ioctl?
  unsigned long sectors = 0;
  ret = ::ioctl(fd, BLKGETSIZE, &sectors);
  bdev_sz = sectors * 512ULL;
#else
#error "Compile error: we don't know how to get the size of a raw block device."
#endif
  if (ret) {
    dout(0) << __func__ << ": failed to read block device size." << dendl;
    return -EIO;
  }

  /* Check for bdev_sz too small */
  if (bdev_sz < ONE_MEG) {
    dout(0) << __func__ << ": your block device must be at least "
      << ONE_MEG << " bytes to be used for a Ceph journal." << dendl;
    return -EINVAL;
  }

  int64_t conf_journal_sz(g_conf.osd_journal_size);
  conf_journal_sz <<= 20;
  if (bdev_sz < conf_journal_sz) {
    dout(0) << __func__ << ": you have configured a journal of size "
      << conf_journal_sz << ", but the block device " << fn << " is only "
      << bdev_sz << " bytes in size." << dendl;
    return -EINVAL;
  }

  if (conf_journal_sz == 0) {
    dout(10) << __func__ << ": no journal size specified in configuration. "
      << "We'll use the entire block device (size: " << bdev_sz << ")" << dendl;
    max_size = bdev_sz;
  }
  else {
    dout(10) << __func__ << ": using " << conf_journal_sz << " bytes "
             << "of a " << bdev_sz << " byte block device." << dendl;
    max_size = conf_journal_sz;
  }

  /* block devices have to write in blocks of PAGE_SIZE */
  block_size = PAGE_SIZE;

  _check_disk_write_cache();
  return 0;
}

static int get_kernel_version(int *a, int *b, int *c)
{
  int ret;
  char buf[128];
  memset(buf, 0, sizeof(buf));
  int fd = TEMP_FAILURE_RETRY(::open("/proc/version", O_RDONLY));
  if (fd < 0) {
    ret = errno;
    derr << "get_kernel_version: failed to open /proc/version: "
	 << cpp_strerror(ret) << dendl;
    goto out;
  }
  ret = safe_read(fd, buf, sizeof(buf) - 1);
  if (ret < 0) {
    derr << "get_kernel_version: failed to read from /proc/version: "
	 << cpp_strerror(ret) << dendl;
    goto close_fd;
  }

  if (sscanf(buf, "Linux version %d.%d.%d", a, b, c) != 3) {
    derr << "get_kernel_version: failed to parse string: '"
	 << buf << "'" << dendl;
    ret = EIO;
    goto close_fd;
  }

  dout(0) << " kernel version is " << *a <<"." << *b << "." << *c << dendl;
  ret = 0;

close_fd:
  TEMP_FAILURE_RETRY(::close(fd));
out:
  return ret;
}

void FileJournal::_check_disk_write_cache() const
{
  ostringstream hdparm_cmd;
  FILE *fp = NULL;
  int a, b, c;

  if (geteuid() != 0) {
    dout(10) << "_check_disk_write_cache: not root, NOT checking disk write "
      << "cache on raw block device " << fn << dendl;
    goto done;
  }

  hdparm_cmd << "/sbin/hdparm -W " << fn;
  fp = popen(hdparm_cmd.str().c_str(), "r");
  if (!fp) {
    dout(10) << "_check_disk_write_cache: failed to run /sbin/hdparm: NOT "
      << "checking disk write cache on raw block device " << fn << dendl;
    goto done;
  }

  while (true) {
    char buf[256];
    memset(buf, 0, sizeof(buf));
    char *line = fgets(buf, sizeof(buf) - 1, fp);
    if (!line) {
      if (ferror(fp)) {
	int ret = -errno;
	derr << "_check_disk_write_cache: fgets error: " << cpp_strerror(ret)
	     << dendl;
	goto close_f;
      }
      else {
	// EOF.
	break;
      }
    }

    int on;
    if (sscanf(line, " write-caching =  %d", &on) != 1)
      continue;
    if (!on) {
      dout(10) << "_check_disk_write_cache: disk write cache is off (good) on "
	       << fn << dendl;
      break;
    }

    // is our kernel new enough?
    if (get_kernel_version(&a, &b, &c)) {
      dout(10) << "_check_disk_write_cache: failed to get kernel version."
	       << dendl;
    }
    else if (a >= 2 && b >= 6 && c >= 33) {
      dout(20) << "_check_disk_write_cache: disk write cache is on, but your "
	       << "kernel is new enough to handle it correctly. (fn:"
	       << fn << ")" << dendl;
      break;
    }
    derr << TEXT_RED
	 << " ** WARNING: disk write cache is ON on " << fn << ".\n"
	 << "    Journaling will not be reliable on kernels prior to 2.6.33\n"
	 << "    (recent kernels are safe).  You can disable the write cache with\n"
	 << "    'hdparm -W 0 " << fn << "'"
	 << TEXT_NORMAL
	 << dendl;
    break;
  }

close_f:
  if (::fclose(fp)) {
    int ret = -errno;
    derr << "_check_disk_write_cache: fclose error: " << cpp_strerror(ret)
	 << dendl;
  }
done:
  ;
}

int FileJournal::_open_file(int64_t oldsize, blksize_t blksize,
			    bool create)
{
  int ret;
  int64_t conf_journal_sz(g_conf.osd_journal_size);
  conf_journal_sz <<= 20;

  if ((g_conf.osd_journal_size == 0) && (oldsize < ONE_MEG)) {
    derr << "I'm sorry, I don't know how large of a journal to create."
	 << "Please specify a block device to use as the journal OR "
	 << "set osd_journal_size in your ceph.conf" << dendl;
    return -EINVAL;
  }

  if (create && (oldsize < conf_journal_sz)) {
    uint64_t newsize(g_conf.osd_journal_size);
    newsize <<= 20;
    dout(10) << "_open extending to " << newsize << " bytes" << dendl;
    ret = ::ftruncate(fd, newsize);
    if (ret < 0) {
      int err = errno;
      derr << "FileJournal::_open_file : unable to extend journal to "
	   << newsize << " bytes: " << cpp_strerror(err) << dendl;
      return -err;
    }
    max_size = newsize;
  }
  else {
    max_size = oldsize;
  }
  block_size = MAX(blksize, (blksize_t)PAGE_SIZE);

  dout(10) << "_open journal is not a block device, NOT checking disk "
           << "write cache on '" << fn << "'" << dendl;

  return 0;
}

int FileJournal::create()
{
  void *buf = 0;
  int64_t needed_space;
  int ret;
  buffer::ptr bp;
  dout(2) << "create " << fn << dendl;

  ret = _open(true, true);
  if (ret < 0)
    goto done;

  // write empty header
  memset(&header, 0, sizeof(header));
  header.clear();
  header.fsid = fsid;
  header.max_size = max_size;
  header.block_size = block_size;
  if (g_conf.journal_block_align || directio)
    header.alignment = block_size;
  else
    header.alignment = 16;  // at least stay word aligned on 64bit machines...
  header.start = get_top();
  print_header();

  bp = prepare_header();
  if (TEMP_FAILURE_RETRY(::pwrite(fd, bp.c_str(), bp.length(), 0)) < 0) {
    ret = errno;
    derr << "FileJournal::create : create write header error "
         << cpp_strerror(ret) << dendl;
    goto close_fd;
  }

  // zero first little bit, too.
  ret = posix_memalign(&buf, block_size, block_size);
  if (ret) {
    derr << "FileJournal::create: failed to allocate " << block_size
	 << " bytes of memory: " << cpp_strerror(ret) << dendl;
    goto close_fd;
  }
  memset(buf, 0, block_size);
  if (TEMP_FAILURE_RETRY(::pwrite(fd, buf, block_size, get_top())) < 0) {
    ret = errno;
    derr << "FileJournal::create: error zeroing first " << block_size
	 << " bytes " << cpp_strerror(ret) << dendl;
    goto free_buf;
  }

  needed_space = g_conf.osd_max_write_size << 20;
  needed_space += (2 * sizeof(entry_header_t)) + get_top();
  if (header.max_size - header.start < needed_space) {
    derr << "FileJournal::create: OSD journal is not large enough to hold "
	 << "osd_max_write_size bytes!" << dendl;
    ret = -ENOSPC;
    goto free_buf;
  }

  dout(2) << "create done" << dendl;
  ret = 0;

free_buf:
  free(buf);
  buf = 0;
close_fd:
  if (TEMP_FAILURE_RETRY(::close(fd)) < 0) {
    ret = errno;
    derr << "FileJournal::create: error closing fd: " << cpp_strerror(ret)
	 << dendl;
    goto done;
  }
done:
  fd = -1;
  return ret;
}

int FileJournal::open(uint64_t next_seq)
{
  dout(2) << "open " << fn << " next_seq " << next_seq << dendl;

  int err = _open(false);
  if (err < 0) 
    return err;

  // assume writeable, unless...
  read_pos = 0;
  write_pos = get_top();

  // read header?
  err = read_header();
  if (err < 0)
    return err;
  dout(10) << "open header.fsid = " << header.fsid 
    //<< " vs expected fsid = " << fsid 
	   << dendl;
  if (header.fsid != fsid) {
    derr << "FileJournal::open: open fsid doesn't match, invalid "
         << "(someone else's?) journal" << dendl;
    return -EINVAL;
  }
  if (header.max_size > max_size) {
    dout(2) << "open journal size " << header.max_size << " > current " << max_size << dendl;
    return -EINVAL;
  }
  if (header.block_size != block_size) {
    dout(2) << "open journal block size " << header.block_size << " != current " << block_size << dendl;
    return -EINVAL;
  }
  if (header.max_size % header.block_size) {
    dout(2) << "open journal max size " << header.max_size
	    << " not a multiple of block size " << header.block_size << dendl;
    return -EINVAL;
  }
  if (header.alignment != block_size && directio) {
    dout(0) << "open journal alignment " << header.alignment << " does not match block size " 
	    << block_size << " (required for direct_io journal mode)" << dendl;
    return -EINVAL;
  }
  if ((header.alignment % PAGE_SIZE) && directio) {
    dout(0) << "open journal alignment " << header.alignment << " is not multiple of page size " << PAGE_SIZE
	    << " (required for direct_io journal mode)" << dendl;
    return -EINVAL;
  }

  // looks like a valid header.
  write_pos = 0;  // not writeable yet

  // find next entry
  read_pos = header.start;
  uint64_t seq = 0;
  while (1) {
    bufferlist bl;
    off64_t old_pos = read_pos;
    if (!read_entry(bl, seq)) {
      dout(10) << "open reached end of journal." << dendl;
      break;
    }
    if (seq > next_seq) {
      dout(10) << "open entry " << seq << " len " << bl.length() << " > next_seq " << next_seq
	       << ", ignoring journal contents"
	       << dendl;
      read_pos = -1;
      last_committed_seq = 0;
      seq = 0;
      return 0;
    }
    if (seq == next_seq) {
      dout(10) << "open reached seq " << seq << dendl;
      read_pos = old_pos;
      break;
    }
    seq++;  // next event should follow.
  }

  return 0;
}

void FileJournal::close()
{
  dout(1) << "close " << fn << dendl;

  // stop writer thread
  stop_writer();

  // close
  assert(writeq.empty());
  assert(fd >= 0);
  TEMP_FAILURE_RETRY(::close(fd));
  fd = -1;
}

void FileJournal::start_writer()
{
  write_stop = false;
  write_thread.create();
}

void FileJournal::stop_writer()
{
  write_lock.Lock();
  {
    write_stop = true;
    write_cond.Signal();
  } 
  write_lock.Unlock();
  write_thread.join();
}



void FileJournal::print_header()
{
  dout(10) << "header: block_size " << header.block_size
	   << " alignment " << header.alignment
	   << " max_size " << header.max_size
	   << dendl;
  dout(10) << "header: start " << header.start << dendl;
  dout(10) << " write_pos " << write_pos << dendl;
}

int FileJournal::read_header()
{
  int r;
  dout(10) << "read_header" << dendl;
  if (directio) {
    buffer::ptr bp = buffer::create_page_aligned(block_size);
    bp.zero();
    r = ::pread(fd, bp.c_str(), bp.length(), 0);
    memcpy(&header, bp.c_str(), sizeof(header));
  } else {
    memset(&header, 0, sizeof(header));  // zero out (read may fail)
    r = ::pread(fd, &header, sizeof(header), 0);
  }
  if (r < 0) {
    char buf[80];
    dout(0) << "read_header error " << errno << " " << strerror_r(errno, buf, sizeof(buf)) << dendl;
    return -errno;
  }
  print_header();

  return 0;
}

bufferptr FileJournal::prepare_header()
{
  bufferptr bp = buffer::create_page_aligned(get_top());
  bp.zero();
  memcpy(bp.c_str(), &header, sizeof(header));
  return bp;
}



int FileJournal::check_for_full(uint64_t seq, off64_t pos, off64_t size)
{
  // already full?
  if (full_state != FULL_NOTFULL)
    return -ENOSPC;

  // take 1 byte off so that we only get pos == header.start on EMPTY, never on FULL.
  off64_t room;
  if (pos >= header.start)
    room = (header.max_size - pos) + (header.start - get_top()) - 1;
  else
    room = header.start - pos - 1;
  dout(10) << "room " << room << " max_size " << max_size << " pos " << pos << " header.start " << header.start
	   << " top " << get_top() << dendl;

  if (do_sync_cond) {
    if (room < (header.max_size >> 1) &&
	room + size > (header.max_size >> 1)) {
      dout(10) << " passing half full mark, triggering commit" << dendl;
      do_sync_cond->Signal();  // initiate a real commit so we can trim
    }
  }

  if (room >= size) {
    dout(10) << "check_for_full at " << pos << " : " << size << " < " << room << dendl;
    if (pos + size > header.max_size)
      must_write_header = true;
    return 0;
  }

  // full
  dout(1) << "check_for_full at " << pos << " : JOURNAL FULL "
	  << pos << " >= " << room
	  << " (max_size " << header.max_size << " start " << header.start << ")"
	  << dendl;

  off64_t max = header.max_size - get_top();
  if (size > max)
    dout(0) << "JOURNAL TOO SMALL: item " << size << " > journal " << max << " (usable)" << dendl;

  return -ENOSPC;
}

int FileJournal::prepare_multi_write(bufferlist& bl, uint64_t& orig_ops, uint64_t& orig_bytes)
{
  // gather queued writes
  off64_t queue_pos = write_pos;

  int eleft = g_conf.journal_max_write_entries;
  unsigned bmax = g_conf.journal_max_write_bytes;

  if (full_state != FULL_NOTFULL)
    return -ENOSPC;
  
  while (!writeq.empty()) {
    int r = prepare_single_write(bl, queue_pos, orig_ops, orig_bytes);
    if (r == -ENOSPC) {
      if (orig_ops)
	break;         // commit what we have

      if (wait_on_full) {
	dout(20) << "prepare_multi_write full on first entry, need to wait" << dendl;
      } else {
	dout(20) << "prepare_multi_write full on first entry, restarting journal" << dendl;

	// throw out what we have so far
	full_state = FULL_FULL;
	while (!writeq.empty()) {
	  dout(30) << "XXX throttle put " << writeq.front().bl.length() << dendl;
	  throttle_ops.put(1);
	  throttle_bytes.put(writeq.front().bl.length());
	  writeq.pop_front();
	}  
	print_header();
      }

      return -ENOSPC;  // hrm, full on first op
    }

    if (eleft) {
      if (--eleft == 0) {
	dout(20) << "prepare_multi_write hit max events per write " << g_conf.journal_max_write_entries << dendl;
	break;
      }
    }
    if (bmax) {
      if (bl.length() >= bmax) {
	dout(20) << "prepare_multi_write hit max write size " << g_conf.journal_max_write_bytes << dendl;
	break;
      }
    }
  }

  dout(20) << "prepare_multi_write queue_pos now " << queue_pos << dendl;
  //assert(write_pos + bl.length() == queue_pos);
  return 0;
}

/*
void FileJournal::queue_write_fin(uint64_t seq, Context *fin)
{
  writing_seq.push_back(seq);
  if (!waiting_for_notfull.empty()) {
    // make sure previously unjournaled stuff waiting for UNFULL triggers
    // _before_ newly journaled stuff does
    dout(10) << "queue_write_fin will defer seq " << seq << " callback " << fin
	     << " until after UNFULL" << dendl;
    C_Gather *g = new C_Gather(writeq.front().fin);
    writing_fin.push_back(g->new_sub());
    waiting_for_notfull.push_back(g->new_sub());
  } else {
    writing_fin.push_back(writeq.front().fin);
    dout(20) << "queue_write_fin seq " << seq << " callback " << fin << dendl;
  }
}
*/

void FileJournal::queue_completions_thru(uint64_t seq)
{
  while (!completions.empty() &&
	 completions.front().first <= seq) {
    dout(10) << "queue_completions_thru seq " << seq << " queueing seq " << completions.front().first
	     << " " << completions.front().second << dendl;
    finisher->queue(completions.front().second);
    completions.pop_front();
  }
}

int FileJournal::prepare_single_write(bufferlist& bl, off64_t& queue_pos, uint64_t& orig_ops, uint64_t& orig_bytes)
{
  // grab next item
  uint64_t seq = writeq.front().seq;
  bufferlist &ebl = writeq.front().bl;
  unsigned head_size = sizeof(entry_header_t);
  off64_t base_size = 2*head_size + ebl.length();

  int alignment = writeq.front().alignment; // we want to start ebl with this alignment
  unsigned pre_pad = 0;
  if (alignment >= 0)
    pre_pad = (alignment - head_size) & ~PAGE_MASK;
  off64_t size = ROUND_UP_TO(base_size + pre_pad, header.alignment);
  unsigned post_pad = size - base_size - pre_pad;

  int r = check_for_full(seq, queue_pos, size);
  if (r < 0)
    return r;   // ENOSPC or EAGAIN

  orig_bytes += ebl.length();
  orig_ops++;

  // add to write buffer
  dout(15) << "prepare_single_write " << orig_ops << " will write " << queue_pos << " : seq " << seq
	   << " len " << ebl.length() << " -> " << size
	   << " (head " << head_size << " pre_pad " << pre_pad
	   << " ebl " << ebl.length() << " post_pad " << post_pad << " tail " << head_size << ")"
	   << " (ebl alignment " << alignment << ")"
	   << dendl;
    
  // add it this entry
  entry_header_t h;
  h.seq = seq;
  h.pre_pad = pre_pad;
  h.len = ebl.length();
  h.post_pad = post_pad;
  h.make_magic(queue_pos, header.fsid);

  bl.append((const char*)&h, sizeof(h));
  if (pre_pad) {
    bufferptr bp = buffer::create_static(pre_pad, zero_buf);
    bl.push_back(bp);
  }
  bl.claim_append(ebl);
  if (h.post_pad) {
    bufferptr bp = buffer::create_static(post_pad, zero_buf);
    bl.push_back(bp);
  }
  bl.append((const char*)&h, sizeof(h));

  // pop from writeq
  writeq.pop_front();
  journalq.push_back(pair<uint64_t,off64_t>(seq, queue_pos));
  writing_seq = seq;

  queue_pos += size;
  if (queue_pos > header.max_size)
    queue_pos = queue_pos + get_top() - header.max_size;

  return 0;
}

int FileJournal::write_bl(off64_t& pos, bufferlist& bl)
{
  // make sure list segments are page aligned
  if (directio && (!bl.is_page_aligned() ||
		   !bl.is_n_page_sized())) {
    bl.rebuild_page_aligned();
    if ((bl.length() & ~PAGE_MASK) != 0 ||
	(pos & ~PAGE_MASK) != 0)
      dout(0) << "rebuild_page_aligned failed, " << bl << dendl;
    assert((bl.length() & ~PAGE_MASK) == 0);
    assert((pos & ~PAGE_MASK) == 0);
  }

  ::lseek64(fd, pos, SEEK_SET);
  int ret = bl.write_fd(fd);
  if (ret) {
    derr << "FileJournal::write_bl : write_fd failed: "
	 << cpp_strerror(ret) << dendl;
    return ret;
  }
  pos += bl.length();
  return 0;
}

void FileJournal::do_write(bufferlist& bl)
{
  // nothing to do?
  if (bl.length() == 0 && !must_write_header) 
    return;

  buffer::ptr hbp;
  if (must_write_header) {
    must_write_header = false;
    hbp = prepare_header();
  }

  writing = true;

  header_t old_header = header;

  write_lock.Unlock();

  dout(15) << "do_write writing " << write_pos << "~" << bl.length() 
	   << (hbp.length() ? " + header":"")
	   << dendl;
  
  utime_t from = g_clock.now();

  // entry
  off64_t pos = write_pos;

  // split?
  off64_t split = 0;
  if (pos + bl.length() > header.max_size) {
    bufferlist first, second;
    split = header.max_size - pos;
    first.substr_of(bl, 0, split);
    second.substr_of(bl, split, bl.length() - split);
    assert(first.length() + second.length() == bl.length());
    dout(10) << "do_write wrapping, first bit at " << pos << " len " << first.length()
	     << " second bit len " << second.length() << " (orig len " << bl.length() << ")" << dendl;

    if (write_bl(pos, first)) {
      derr << "FileJournal::do_write: write_bl(pos=" << pos
	   << ") failed" << dendl;
      ceph_abort();
    }
    assert(pos == header.max_size);
    if (hbp.length()) {
      // be sneaky: include the header in the second fragment
      second.push_front(hbp);
      pos = 0;          // we included the header
    } else
      pos = get_top();  // no header, start after that
    if (write_bl(pos, second)) {
      derr << "FileJournal::do_write: write_bl(pos=" << pos
	   << ") failed" << dendl;
      ceph_abort();
    }
  } else {
    // header too?
    if (hbp.length()) {
      if (TEMP_FAILURE_RETRY(::pwrite(fd, hbp.c_str(), hbp.length(), 0)) < 0) {
	int err = errno;
	derr << "FileJournal::do_write: pwrite(fd=" << fd
	     << ", hbp.length=" << hbp.length() << ") failed :"
	     << cpp_strerror(err) << dendl;
	ceph_abort();
      }
    }

    if (write_bl(pos, bl)) {
      derr << "FileJournal::do_write: write_bl(pos=" << pos
	   << ") failed" << dendl;
      ceph_abort();
    }
  }

  if (!directio) {
    dout(20) << "do_write fsync" << dendl;
#ifdef DARWIN
    ::fsync(fd);
#else
# ifdef HAVE_SYNC_FILE_RANGE
    if (is_bdev) {
      if (split) {
	::sync_file_range(fd, header.max_size - split, split, SYNC_FILE_RANGE_WAIT_BEFORE|SYNC_FILE_RANGE_WRITE);
	::sync_file_range(fd, get_top(), bl.length() - split, SYNC_FILE_RANGE_WAIT_BEFORE|SYNC_FILE_RANGE_WRITE);
	::sync_file_range(fd, header.max_size - split, split, SYNC_FILE_RANGE_WAIT_AFTER);
	::sync_file_range(fd, get_top(), bl.length() - split, SYNC_FILE_RANGE_WAIT_AFTER);
      } else {
	::sync_file_range(fd, write_pos, bl.length(),
			  SYNC_FILE_RANGE_WAIT_BEFORE|SYNC_FILE_RANGE_WRITE|SYNC_FILE_RANGE_WAIT_AFTER);
      }
    } else
# endif
      ::fdatasync(fd);
#endif
  }

  utime_t lat = g_clock.now() - from;    
  dout(20) << "do_write latency " << lat << dendl;

  write_lock.Lock();    

  writing = false;

  // wrap if we hit the end of the journal
  if (pos == header.max_size)
    pos = get_top();
  write_pos = pos;
  assert(write_pos % header.alignment == 0);

  journaled_seq = writing_seq;

  // kick finisher?  
  //  only if we haven't filled up recently!
  if (full_state != FULL_NOTFULL) {
    dout(10) << "do_write NOT queueing finisher seq " << journaled_seq
	     << ", full_commit_seq|full_restart_seq" << dendl;
  } else {
    if (plug_journal_completions) {
      dout(20) << "do_write NOT queueing finishers through seq " << journaled_seq
	       << " due to completion plug" << dendl;
    } else {
      dout(20) << "do_write queueing finishers through seq " << journaled_seq << dendl;
      queue_completions_thru(journaled_seq);
    }
  }
}

void FileJournal::flush()
{
  write_lock.Lock();
  while ((!writeq.empty() || writing) && !write_stop) {
    dout(10) << "flush waiting for writeq to empty and writes to complete" << dendl;
    write_empty_cond.Wait(write_lock);
  }
  write_lock.Unlock();
  dout(10) << "flush waiting for finisher" << dendl;
  finisher->wait_for_empty();
  dout(10) << "flush done" << dendl;
}


void FileJournal::write_thread_entry()
{
  dout(10) << "write_thread_entry start" << dendl;
  write_lock.Lock();
  
  while (!write_stop || 
	 !writeq.empty()) {  // ensure we fully flush the writeq before stopping
    if (writeq.empty()) {
      // sleep
      dout(20) << "write_thread_entry going to sleep" << dendl;
      write_empty_cond.Signal();
      write_cond.Wait(write_lock);
      dout(20) << "write_thread_entry woke up" << dendl;
      continue;
    }
    
    uint64_t orig_ops = 0;
    uint64_t orig_bytes = 0;

    bufferlist bl;
    int r = prepare_multi_write(bl, orig_ops, orig_bytes);
    if (r == -ENOSPC) {
      dout(20) << "write_thread_entry full, going to sleep (waiting for commit)" << dendl;
      commit_cond.Wait(write_lock);
      dout(20) << "write_thread_entry woke up" << dendl;
      continue;
    }
    assert(r == 0);
    do_write(bl);
    
    dout(30) << "XXX throttle put " << orig_bytes << dendl;
    uint64_t new_ops = throttle_ops.put(orig_ops);
    uint64_t new_bytes = throttle_bytes.put(orig_bytes);
    dout(10) << "write_thread throttle finished " << orig_ops << " ops and " 
	     << orig_bytes << " bytes, now "
	     << new_ops << " ops and " << new_bytes << " bytes"
	     << dendl;
  }
  write_empty_cond.Signal();
  write_lock.Unlock();
  dout(10) << "write_thread_entry finish" << dendl;
}


void FileJournal::submit_entry(uint64_t seq, bufferlist& e, int alignment, Context *oncommit)
{
  Mutex::Locker locker(write_lock);  // ** lock **

  // dump on queue
  dout(10) << "submit_entry seq " << seq
	   << " len " << e.length()
	   << " (" << oncommit << ")" << dendl;

  if (oncommit)
    completions.push_back(pair<uint64_t,Context*>(seq, oncommit));

  if (full_state == FULL_NOTFULL) {
    // queue and kick writer thread
    dout(30) << "XXX throttle take " << e.length() << dendl;
    throttle_ops.take(1);
    throttle_bytes.take(e.length());

    writeq.push_back(write_item(seq, e, alignment));
    write_cond.Signal();
  } else {
    // not journaling this.  restart writing no sooner than seq + 1.
    dout(10) << " journal is/was full" << dendl;
  }
}

void FileJournal::commit_start()
{
  dout(10) << "commit_start" << dendl;

  // was full?
  switch (full_state) {
  case FULL_NOTFULL:
    break; // all good

  case FULL_FULL:
    dout(1) << " FULL_FULL -> FULL_WAIT.  last commit epoch committed, waiting for a new one to start." << dendl;
    full_state = FULL_WAIT;
    break;

  case FULL_WAIT:
    dout(1) << " FULL_WAIT -> FULL_NOTFULL.  journal now active, setting completion plug." << dendl;
    full_state = FULL_NOTFULL;
    plug_journal_completions = true;
    break;
  }
}

void FileJournal::committed_thru(uint64_t seq)
{
  Mutex::Locker locker(write_lock);

  if (seq < last_committed_seq) {
    dout(10) << "committed_thru " << seq << " < last_committed_seq " << last_committed_seq << dendl;
    assert(seq >= last_committed_seq);
    return;
  }
  if (seq == last_committed_seq) {
    dout(10) << "committed_thru " << seq << " == last_committed_seq " << last_committed_seq << dendl;
    return;
  }

  dout(10) << "committed_thru " << seq << " (last_committed_seq " << last_committed_seq << ")" << dendl;
  last_committed_seq = seq;

  // adjust start pointer
  while (!journalq.empty() && journalq.front().first <= seq) {
    journalq.pop_front();
  }
  if (!journalq.empty()) {
    header.start = journalq.front().second;
  } else {
    header.start = write_pos;
  }
  must_write_header = true;
  print_header();

  // completions!
  queue_completions_thru(seq);
  if (plug_journal_completions) {
    dout(10) << " removing completion plug, queuing completions thru journaled_seq " << journaled_seq << dendl;
    plug_journal_completions = false;
    queue_completions_thru(journaled_seq);
  }

  // committed but unjournaled items
  while (!writeq.empty() && writeq.front().seq <= seq) {
    dout(15) << " dropping committed but unwritten seq " << writeq.front().seq 
	     << " len " << writeq.front().bl.length()
	     << dendl;
    dout(30) << "XXX throttle put " << writeq.front().bl.length() << dendl;
    throttle_ops.put(1);
    throttle_bytes.put(writeq.front().bl.length());
    writeq.pop_front();  
  }
  
  commit_cond.Signal();

  dout(10) << "committed_thru done" << dendl;
}


void FileJournal::make_writeable()
{
  _open(true);

  if (read_pos > 0)
    write_pos = read_pos;
  else
    write_pos = get_top();
  read_pos = 0;

  must_write_header = true;
  start_writer();
}

void FileJournal::wrap_read_bl(off64_t& pos, int64_t olen, bufferlist& bl)
{
  while (olen > 0) {
    while (pos >= header.max_size)
      pos = pos + get_top() - header.max_size;

    int64_t len;
    if (pos + olen > header.max_size)
      len = header.max_size - pos;        // partial
    else
      len = olen;                         // rest
    
#ifdef DARWIN
    ::lseek(fd, pos, SEEK_SET);
#else
    ::lseek64(fd, pos, SEEK_SET);
#endif
    
    bufferptr bp = buffer::create(len);
    int r = safe_read_exact(fd, bp.c_str(), len);
    if (r) {
      derr << "FileJournal::wrap_read_bl: safe_read_exact returned "
	   << r << dendl;
      ceph_abort();
    }
    bl.push_back(bp);
    pos += len;
    olen -= len;
  }
}

bool FileJournal::read_entry(bufferlist& bl, uint64_t& seq)
{
  if (!read_pos) {
    dout(2) << "read_entry -- not readable" << dendl;
    return false;
  }

  off64_t pos = read_pos;
  bl.clear();

  // header
  entry_header_t *h;
  bufferlist hbl;
  wrap_read_bl(pos, sizeof(*h), hbl);
  h = (entry_header_t *)hbl.c_str();

  if (!h->check_magic(read_pos, header.fsid)) {
    dout(2) << "read_entry " << read_pos << " : bad header magic, end of journal" << dendl;
    return false;
  }

  // pad + body + pad
  if (h->pre_pad)
    pos += h->pre_pad;
  wrap_read_bl(pos, h->len, bl);
  if (h->post_pad)
    pos += h->post_pad;

  // footer
  entry_header_t *f;
  bufferlist fbl;
  wrap_read_bl(pos, sizeof(*f), fbl);
  f = (entry_header_t *)fbl.c_str();
  if (memcmp(f, h, sizeof(*f))) {
    dout(2) << "read_entry " << read_pos << " : bad footer magic, partial entry, end of journal" << dendl;
    return false;
  }

  // yay!
  dout(1) << "read_entry " << read_pos << " : seq " << h->seq
	  << " " << h->len << " bytes"
	  << dendl;

  if (seq && h->seq < seq) {
    dout(2) << "read_entry " << read_pos << " : got seq " << h->seq << ", expected " << seq << ", stopping" << dendl;
    return false;
  }

  if (h->seq < last_committed_seq) {
    dout(0) << "read_entry seq " << seq << " < last_committed_seq " << last_committed_seq << dendl;
    assert(h->seq >= last_committed_seq);
    return false;
  }
  last_committed_seq = h->seq;

  // ok!
  seq = h->seq;
  journalq.push_back(pair<uint64_t,off64_t>(h->seq, read_pos));

  read_pos = pos;
  assert(read_pos % header.alignment == 0);
 
  return true;
}

void FileJournal::throttle()
{
  if (throttle_ops.wait(g_conf.journal_queue_max_ops))
    dout(1) << "throttle: waited for ops" << dendl;
  if (throttle_bytes.wait(g_conf.journal_queue_max_bytes))
    dout(1) << "throttle: waited for bytes" << dendl;
}
