#include <boost/filesystem.hpp>
#include "log/LttngStream.h"

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

inline lttng_stream& operator<<(lttng_stream& os, const boost::filesystem::path& p)
{
  return os << boost::io::quoted(p.template string<std::basic_string<char> >(), static_cast<char>('&'));
}
