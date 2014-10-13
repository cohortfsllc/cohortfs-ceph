// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LOG_LTTNGSTREAM
#define CEPH_LOG_LTTNGSTREAM

#include <inttypes.h>
#include <ostream>
#include <iomanip>
#include <boost/intrusive_ptr.hpp>
#include <memory>
#include <boost/filesystem.hpp>
//#include <boost/io/ios_state.hpp>



// lttng log stream
class lttng_stream {
private:
    int pid;
    int message_id;
    pthread_t thread;

  // integer types for log_integer tracepoint
  enum int_type {
    TYPE_BOOL,
    TYPE_U8,
    TYPE_S8,
    TYPE_U16,
    TYPE_S16,
    TYPE_U32,
    TYPE_S32,
    TYPE_U64,
    TYPE_S64,
    TYPE_FLOAT,
    TYPE_DOUBLE,
    TYPE_DOUBLE64,
    TYPE_PTR,
  };

  // stream manipulators
  enum manip_type {
    MANIP_ENDL,
    MANIP_ENDS,
    MANIP_FLUSH,
    MANIP_BOOLALPHA,
    MANIP_SHOWBASE,
    MANIP_SHOWPOINT,
    MANIP_SHOWPOS,
    MANIP_UNITBUF,
    MANIP_UPPERCASE,
    MANIP_DEC,
    MANIP_HEX,
    MANIP_OCT,
    MANIP_FIXED,
    MANIP_SCIENTIFIC,
    MANIP_INTERNAL,
    MANIP_LEFT,
    MANIP_RIGHT,
    MANIP_SETBASE,
    MANIP_SETFILL,
    MANIP_SETPRECISION,
    MANIP_SETW,
    MANIP_SETIOSFLAGS,
    MANIP_RESETIOSFLAGS,
  };

  // tracepoint emission functions
  void emit_header(int entity_type, const char *entity_name, long thread, 
	short subsys, short prio);
  void emit_integer(uint64_t val, enum int_type type);
  void emit_string(const char *val);
  void emit_manip(enum manip_type type, int val);
  void emit_footer();
  void emit_blob(const char *, size_t len);

public:
  lttng_stream(int entity_type, const char *entity_name,
	 short subsys, short prio);


  ~lttng_stream()
  {
    emit_footer();
  }

  // output operators for built-in types
  lttng_stream& operator<<(bool val) {
    emit_integer(static_cast<uint64_t>(val), TYPE_BOOL);
    return *this;
  }
  lttng_stream& operator<<(char val) {
    emit_integer(static_cast<uint64_t>(val), TYPE_S8);
    return *this;
  }
  lttng_stream& operator<<(unsigned char val) {
    emit_integer(static_cast<uint64_t>(val), TYPE_U8);
    return *this;
  }
  lttng_stream& operator<<(short val) {
    emit_integer(static_cast<uint64_t>(val), TYPE_S16);
    return *this;
  }
  lttng_stream& operator<<(unsigned short val) {
    emit_integer(static_cast<uint64_t>(val), TYPE_U16);
    return *this;
  }
  lttng_stream& operator<<(int val) {
    emit_integer(static_cast<uint64_t>(val), TYPE_S32);
    return *this;
  }
  lttng_stream& operator<<(unsigned int val) {
    emit_integer(static_cast<uint64_t>(val), TYPE_U32);
    return *this;
  }
  lttng_stream& operator<<(long val) {
    const enum int_type type = sizeof(val) == 4 ? TYPE_S32 : TYPE_S64;
    emit_integer(static_cast<uint64_t>(val), type);
    return *this;
  }
  lttng_stream& operator<<(unsigned long val) {
    const enum int_type type = sizeof(val) == 4 ? TYPE_U32 : TYPE_U64;
    emit_integer(static_cast<uint64_t>(val), type);
    return *this;
  }
  lttng_stream& operator<<(long long val) {
    emit_integer(static_cast<uint64_t>(val), TYPE_S64);
    return *this;
  }
  lttng_stream& operator<<(unsigned long long val) {
    emit_integer(static_cast<uint64_t>(val), TYPE_U64);
    return *this;
  }
  lttng_stream& operator<<(float val) {
    emit_integer(static_cast<uint64_t>(val), TYPE_FLOAT);
    return *this;
  }
  lttng_stream& operator<<(double val) {
    emit_integer(static_cast<uint64_t>(val), TYPE_DOUBLE);
    return *this;
  }
  lttng_stream& operator<<(long double val) {
    emit_integer(static_cast<uint64_t>(val), TYPE_DOUBLE64);
    return *this;
  }
  lttng_stream& operator<<(void* val) {
    emit_integer(reinterpret_cast<uint64_t>(val), TYPE_PTR);
    return *this;
  }
  lttng_stream& operator<<(const char* val) {
    emit_string(val);
    return *this;
  }
  lttng_stream& operator<<(const unsigned char* val) {
    emit_string(reinterpret_cast<const char*>(val));
    return *this;
  }

  // ostream manip: for endl, ends, flush
  lttng_stream& operator<<(std::ostream& (*manip)(std::ostream&)) {
    typedef std::ostream::char_type C;
    typedef std::ostream::traits_type T;

    if (manip == &std::endl<C, T>) {
      emit_manip(MANIP_ENDL, 0);
    } else if (manip == &std::ends<C, T>) {
      emit_manip(MANIP_ENDS, 0);
    } else if (manip == &std::flush<C, T>) {
      emit_manip(MANIP_FLUSH, 0);
    }
    return *this;
  }

  // ios_base manip: for dec, hex, oct, etc
  lttng_stream& operator<<(std::ios_base& (*manip)(std::ios_base&)) {
    // Format flag manipulators at http://www.cplusplus.com/reference/ios/
    if (manip == std::dec) {
      emit_manip(MANIP_DEC, 0);
    } else if (manip == std::hex) {
      emit_manip(MANIP_HEX, 0);
    } else if (manip == std::oct) {
      emit_manip(MANIP_OCT, 0);
    } else if (manip == std::boolalpha) {
      emit_manip(MANIP_BOOLALPHA, 1);
    } else if (manip == std::noboolalpha) {
      emit_manip(MANIP_BOOLALPHA, 0);
    } else if (manip == std::showbase) {
      emit_manip(MANIP_SHOWBASE, 1);
    } else if (manip == std::noshowbase) {
      emit_manip(MANIP_SHOWBASE, 0);
    } else if (manip == std::showpoint) {
      emit_manip(MANIP_SHOWPOINT, 1);
    } else if (manip == std::noshowpoint) {
      emit_manip(MANIP_SHOWPOINT, 0);
    } else if (manip == std::showpos) {
      emit_manip(MANIP_SHOWPOS, 1);
    } else if (manip == std::noshowpos) {
      emit_manip(MANIP_SHOWPOS, 0);
    } else if (manip == std::uppercase) {
      emit_manip(MANIP_UPPERCASE, 1);
    } else if (manip == std::nouppercase) {
      emit_manip(MANIP_UPPERCASE, 0);
    } else if (manip == std::fixed) {
      emit_manip(MANIP_FIXED, 0);
    } else if (manip == std::scientific) {
      emit_manip(MANIP_SCIENTIFIC, 0);
    } else if (manip == std::internal) {
      emit_manip(MANIP_INTERNAL, 0);
    } else if (manip == std::left) {
      emit_manip(MANIP_LEFT, 0);
    } else if (manip == std::right) {
      emit_manip(MANIP_RIGHT, 0);
    }
    return *this;
  }

  // iomanip: setbase, setfill, setprecision, setw, setiosflags, resetiosflags
  // XXX: glibc type names are not portable
  lttng_stream& operator<<(std::_Setbase b) {
    emit_manip(MANIP_SETBASE, b._M_base);
    return *this;
  }
  lttng_stream& operator<<(std::_Setfill<char> f) {
    emit_manip(MANIP_SETFILL, f._M_c);
    return *this;
  }
  lttng_stream& operator<<(std::_Setprecision p) {
    emit_manip(MANIP_SETPRECISION, p._M_n);
    return *this;
  }
  lttng_stream& operator<<(std::_Setw w) {
    emit_manip(MANIP_SETW, w._M_n);
    return *this;
  }
  lttng_stream& operator<<(std::_Setiosflags f) {
    emit_manip(MANIP_SETIOSFLAGS, f._M_mask);
    return *this;
  }
  lttng_stream& operator<<(std::_Resetiosflags f) {
    emit_manip(MANIP_RESETIOSFLAGS, f._M_mask);
    return *this;
  }
  struct lttng_endl {};
  static lttng_endl endl;

   lttng_stream& write(const char* s, size_t len)
  {
    emit_blob(s, len);
    return *this;
  }
};

inline lttng_stream& operator<<(lttng_stream &out, const std::string &s) {
    return out << s.c_str();
}

//operator
template<class Y>
inline lttng_stream& operator<<(lttng_stream &out, boost::intrusive_ptr<Y> const & p) {
    return out << p.get();  
}

template<class Y>
inline lttng_stream& operator<<(lttng_stream &out, std::shared_ptr<Y> const & p) {
    return out << p.get();  
}

// replaces flush with a no_op in dendl
inline lttng_stream& operator<<(lttng_stream &out, lttng_stream::lttng_endl&) 
{
 return out;
}

inline lttng_stream& basic_string_inserter_imp(lttng_stream& os,
	std::string const & string, char escape, char delim)
{
  os << delim;
typename std::string::const_iterator
    end_it = string.end();
  for (typename std::string::const_iterator
    it = string.begin();
    it != end_it;
    ++it )
{
  if (*it == delim || *it == escape)
    os << escape;
    os << *it;
}
os << delim;
return os;
}

inline lttng_stream& operator<<(lttng_stream& os, 
        const boost::io::detail::quoted_proxy<std::string const &, char>& proxy)
      {
        return basic_string_inserter_imp(os, proxy.string, proxy.escape, proxy.delim);
      }

inline lttng_stream&
operator<<(lttng_stream& os, const boost::filesystem::path& p)
{
  return os
    << boost::io::quoted(p.template string<std::basic_string<char> >(), static_cast<char>('&'));
}

#endif // CEPH_LOG_LTTNGSTREAM


