// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifdef ENCODE_DUMP
# include <typeinfo>
# include <cxxabi.h>
#endif

#include <iostream>

#include "include/types.h"

#include "common/ceph_context.h"
#include "Message.h"
#include "Pipe.h"
#include "MessageFactory.h"

#include "common/config.h"

#define DEBUGLVL  10	// debug level of output

#define dout_subsys ceph_subsys_ms

void Message::encode(uint64_t features, int crcflags)
{
  // encode and copy out of *m
  if (empty_payload()) {
    encode_payload(features);

    // if the encoder didn't specify past compatibility, we assume it
    // is incompatible.
    if (header.compat_version == 0)
      header.compat_version = header.version;
  }
  if (crcflags & MSG_CRC_HEADER)
    calc_front_crc();

  // update envelope
  header.front_len = get_payload().length();
  header.middle_len = get_middle().length();
  header.data_len = get_data().length();
  if (crcflags & MSG_CRC_HEADER)
    calc_header_crc();

  footer.flags = CEPH_MSG_FOOTER_COMPLETE;

  if (crcflags & MSG_CRC_DATA) {
    calc_data_crc();

#ifdef ENCODE_DUMP
    bufferlist bl;
    ::encode(get_header(), bl);

    // dump the old footer format
    ceph_msg_footer_old old_footer;
    old_footer.front_crc = footer.front_crc;
    old_footer.middle_crc = footer.middle_crc;
    old_footer.data_crc = footer.data_crc;
    old_footer.flags = footer.flags;
    ::encode(old_footer, bl);

    ::encode(get_payload(), bl);
    ::encode(get_middle(), bl);
    ::encode(get_data(), bl);

    // this is almost an exponential backoff, except because we count
    // bits we tend to sample things we encode later, which should be
    // more representative.
    static int i = 0;
    i++;
    int bits = 0;
    for (unsigned t = i; t; bits++)
      t &= t - 1;
    if (bits <= 2) {
      char fn[200];
      int status;
      snprintf(fn, sizeof(fn), ENCODE_STRINGIFY(ENCODE_DUMP) "/%s__%d.%x",
	       abi::__cxa_demangle(typeid(*this).name(), 0, 0, &status),
	       getpid(), i++);
      int fd = ::open(fn, O_WRONLY|O_TRUNC|O_CREAT, 0644);
      if (fd >= 0) {
	bl.write_fd(fd);
	::close(fd);
      }
    }
#endif
  } else {
    footer.flags = (unsigned)footer.flags | CEPH_MSG_FOOTER_NOCRC;
  }
}

void Message::dump(Formatter *f) const
{
  stringstream ss;
  print(ss);
  f->dump_string("summary", ss.str());
}

Message *decode_message(CephContext *cct, int crcflags,
			ceph_msg_header& header,
			ceph_msg_footer& footer,
			bufferlist& front, bufferlist& middle,
			bufferlist& data, Connection *conn)
{
  // verify crc
  if (crcflags & MSG_CRC_HEADER) {
    uint32_t front_crc = front.crc32c(0);
    uint32_t middle_crc = middle.crc32c(0);

    if (front_crc != footer.front_crc) {
      if (cct) {
	ldout(cct, 0) << "bad crc in front " << front_crc << " != exp " << footer.front_crc << dendl;
	ldout(cct, 20) << " ";
	front.hexdump(*_dout);
	*_dout << dendl;
      }
      return 0;
    }
    if (middle_crc != footer.middle_crc) {
      if (cct) {
	ldout(cct, 0) << "bad crc in middle " << middle_crc << " != exp " << footer.middle_crc << dendl;
	ldout(cct, 20) << " ";
	middle.hexdump(*_dout);
	*_dout << dendl;
      }
      return 0;
    }

    if ((footer.flags & CEPH_MSG_FOOTER_NOCRC) == 0) {
      uint32_t data_crc = data.crc32c(0);
      if (data_crc != footer.data_crc) {
	if (cct) {
	  ldout(cct, 0) << "bad crc in data " << data_crc << " != exp " << footer.data_crc << dendl;
	  ldout(cct, 20) << " ";
	  data.hexdump(*_dout);
	  *_dout << dendl;
	}
	return 0;
      }
    }
  }

  // make message
  MessageFactory *factory = conn->get_messenger()->get_message_factory();
  Message *m = factory->create(header.type);
  if (m == nullptr) {
    if (cct) {
      ldout(cct, 0) << "can't decode unknown message type " << header.type
          << " MSG_AUTH=" << CEPH_MSG_AUTH << dendl;
      if (cct->_conf->ms_die_on_bad_msg)
        assert(0);
    }
    return 0;
  }

  // m->header.version, if non-zero, should be populated with the
  // newest version of the encoding the code supports.	If set, check
  // it against compat_version.
  if (m->get_header().version &&
      m->get_header().version < header.compat_version) {
    if (cct) {
      ldout(cct, 0) << "will not decode message of type " << header.type
		    << " version " << header.version
		    << " because compat_version " << header.compat_version
		    << " > supported version " << m->get_header().version << dendl;
      if (cct->_conf->ms_die_on_bad_msg)
	assert(0);
    }
    m->put();
    return 0;
  }

  m->set_connection(conn);
  m->set_header(header);
  m->set_footer(footer);
  m->set_payload(front);
  m->set_middle(middle);
  m->set_data(data);

  try {
    m->decode_payload();
  }
  catch (std::system_error &e) {
    if (cct) {
      lderr(cct) << "failed to decode message of type " << header.type
		 << " v" << header.version
		 << ": " << e.what() << dendl;
      ldout(cct, 30) << "dump: \n";
      m->get_payload().hexdump(*_dout);
      *_dout << dendl;
      if (cct->_conf->ms_die_on_bad_msg)
	assert(0);
    }
    m->put();
    return 0;
  }

  if (m->trace) {
    m->trace.event("decode_message");
    {
      ostringstream oss;
      oss << m->get_source_addr();
      m->trace.keyval("From", oss.str().c_str());
    }
    m->trace.keyval("Receiver", cct->_conf->name.to_cstr());
  }

  // done!
  return m;
}


WRITE_RAW_ENCODER(blkin_trace_info)

void Message::encode_trace(bufferlist &bl) const
{
#ifdef HAVE_LTTNG
  ::encode(*trace.get_info(), bl);
#endif
}

void Message::decode_trace(bufferlist::iterator &p,
                           const char *name, bool create)
{
#ifdef HAVE_LTTNG
  Messenger *msgr = connection ? connection->get_messenger() : NULL;
  const ZTracer::Endpoint *endpoint = msgr ? msgr->get_trace_endpoint() : NULL;

  blkin_trace_info info;
  ::decode(info, p);
  if (info.trace_id) {
    trace.init(name, endpoint, &info, true);
  } else if (create) {
    trace.init(name, endpoint);
  }
#endif
}

// This routine is not used for ordinary messages, but only when encapsulating a message
// for forwarding and routing.	It's also used in a backward compatibility test, which only
// effectively tests backward compability for those functions.	To avoid backward compatibility
// problems, we currently always encode and decode using the old footer format that doesn't
// allow for message authentication.  Eventually we should fix that.  PLR

void encode_message(Message *msg, uint64_t features, bufferlist& payload)
{
  bufferlist front, middle, data;
  ceph_msg_footer_old old_footer;
  ceph_msg_footer footer;
  msg->encode(features, true);
  ::encode(msg->get_header(), payload);

  // Here's where we switch to the old footer format.  PLR

  footer = msg->get_footer();
  old_footer.front_crc = footer.front_crc;
  old_footer.middle_crc = footer.middle_crc;
  old_footer.data_crc = footer.data_crc;
  old_footer.flags = footer.flags;
  ::encode(old_footer, payload);

  ::encode(msg->get_payload(), payload);
  ::encode(msg->get_middle(), payload);
  ::encode(msg->get_data(), payload);
}

// See above for somewhat bogus use of the old message footer.	We switch to the current footer
// after decoding the old one so the other form of decode_message() doesn't have to change.
// We've slipped in a 0 signature at this point, so any signature checking after this will
// fail.  PLR

Message *decode_message(CephContext *cct, int crcflags, bufferlist::iterator& p)
{
  ceph_msg_header h;
  ceph_msg_footer_old fo;
  ceph_msg_footer f;
  bufferlist fr, mi, da;
  ::decode(h, p);
  ::decode(fo, p);
  f.front_crc = fo.front_crc;
  f.middle_crc = fo.middle_crc;
  f.data_crc = fo.data_crc;
  f.flags = fo.flags;
  f.sig = 0;
  ::decode(fr, p);
  ::decode(mi, p);
  ::decode(da, p);
  return decode_message(cct, crcflags, h, f, fr, mi, da);
}

