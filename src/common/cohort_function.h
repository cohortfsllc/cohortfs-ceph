// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef COMMON_COHORT_FUNCTION_H
#define COMMON_COHORT_FUNCTION_H

#include <cassert>
#include <exception>
#include <functional>
#include <memory>
#include <tuple>
#include <typeinfo>
#include <type_traits>

#include <boost/compressed_pair.hpp>
#include <boost/intrusive/slist.hpp>

namespace cohort {

  // Invoke

  namespace detail {
    // ANY type
    struct any {
      any(...);
    };

    // Not a type?
    struct nat {
      nat() = delete;
      nat(const nat&) = delete;
      nat& operator=(const nat&) = delete;
      ~nat() = delete;
    };
  };

  // This may be replaced by std::invoke in C++17.

  // Fallback: FAILURE

  template<class ...Args>
  auto invoke(detail::any, Args&& ...args) -> detail::nat;

  // Member functions with arguments

  template<typename Fun, class Arg0, typename... Args>
  auto invoke(Fun&& f, Arg0&& arg0, Args&& ...args)
    -> decltype((std::forward<Arg0>(arg0).*f)
		(std::forward<Args>(args)...)) {
    return (std::forward<Arg0>(arg0).*f)(std::forward<Args>(args)...);
  }

  template<typename Fun, class Arg0, typename...Args>
  auto invoke(Fun&& f, Arg0&& arg0, Args&& ...args)
    -> decltype(((*std::forward<Arg0>(arg0)).*f)
		(std::forward<Args>(args)...)) {
    return ((*std::forward<Arg0>(arg0)).*f)(std::forward<Args>(args)...);
  }

  // Member functions of no arguments

  template<typename Fun, class Arg>
  auto invoke(Fun&& f, Arg&& arg)
    -> decltype(std::forward<Arg>(arg).*f) {
    return std::forward<Arg>(arg).*f;
  }

  template<typename Fun, class Arg>
  auto invoke(Fun&& f, Arg&& arg)
    -> decltype((*std::forward<Arg>(arg)).*f) {
    return (*std::forward<Arg>(arg)).*f;
  }

  // Non-member functions.

  template<class Fun, class ...Args>
  auto invoke(Fun&& f, Args&& ...args)
    -> decltype(std::forward<Fun>(f)(std::forward<Args>(args)...)) {
    return std::forward<Fun>(f)(std::forward<Args>(args)...);
  }


  namespace detail {
    // Check for complete types

    // Gautama H. Buddha this is huge.

    template<typename... T>
    struct check_complete;

    template<>
    struct check_complete<> {
    };

    template <typename H, class T0, class... T>
    struct check_complete<H, T0, T...>
      : private check_complete<H>, private check_complete<T0, T...> {
    };

    template<typename H>
    struct check_complete<H, H> : private check_complete<H> {
    };

    template <typename T>
    struct check_complete<T> {
      static_assert(sizeof(T) > 0, "Type must be complete.");
    };

    template<typename T>
    struct check_complete<T&> : private check_complete<T> {
    };

    template<typename T>
    struct check_complete<T&&> : private check_complete<T> {
    };

    // Odd, this.
    template<typename Res, class ...Params>
    struct check_complete<Res (*)(Params...)> : private check_complete<Res> {
    };

    template<typename ...Params>
    struct check_complete<void (*)(Params...)> {
    };

    template<typename Res, typename ...Params>
    struct check_complete<Res(Params...)> : private check_complete<Res> {
    };

    template<typename ...Params>
    struct check_complete<void(Params...)> {
    };

    template<typename Res, class Class, class ...Params>
    struct check_complete<Res(Class::*)(Params...)>
      : private check_complete<Class> {
    };

    template<typename Res, class Class, typename ...Params>
    struct check_complete<Res(Class::*)(Params...) const>
      : private check_complete<Class> {
    };

    template<typename Res, class Class, typename ...Params>
    struct check_complete<Res(Class::*)(Params...) volatile>
      : private check_complete<Class> {
    };

    template<typename Res, class Class, typename ...Params>
    struct check_complete<Res (Class::*)(Params...) const volatile>
      : private check_complete<Class> {
    };

    template<typename Res, class Class, class ...Param>
    struct check_complete<Res (Class::*)(Param...)&>
      : private check_complete<Class> {
    };

    template<typename Res, class Class, typename ...Params>
    struct check_complete<Res(Class::*)(Params...) const&>
      : private check_complete<Class> {
    };

    template<typename Res, class Class, class ...Params>
    struct check_complete<Res(Class::*)(Params...) volatile&>
      : private check_complete<Class> {
    };

    template<typename Res, class Class, typename ...Params>
    struct check_complete<Res(Class::*)(Params...) const volatile&>
      : private check_complete<Class> {
    };

    template<typename Res, class Class, typename ...Params>
    struct check_complete<Res(Class::*)(Params...) &&>
      : private check_complete<Class> {
    };

    template<typename Res, class Class, typename ...Params>
    struct check_complete<Res(Class::*)(Params...) const&&>
      : private check_complete<Class> {
    };

    template<typename Res, class Class, typename ...Params>
    struct check_complete<Res(Class::*)(Params...) volatile&&>
      : private check_complete<Class> {
    };

    template<typename Res, class Class, typename ...Params>
    struct check_complete<Res(Class::*)(Params...) const volatile&&>
      : private check_complete<Class> {
    };

    template<typename Res, class Class>
    struct check_complete<Res Class::*> : private check_complete<Class> {
    };

    // invokable

    template<typename F, class ...Args>
    struct invokable_imp : private check_complete<F> {
      typedef decltype(
	invoke(std::declval<F>(), std::declval<Args>()...)) type;
      static const bool value = !std::is_same<type, nat>::value;
    };

    // invoke_of

    template<bool Invokable, typename F, typename ...Args>
    struct invoke_of_imp { // false
    };

    template<typename F, typename ...Args>
    struct invoke_of_imp<true, F, Args...> {
      typedef typename invokable_imp<F, Args...>::type type;
    };
  };

  // invokable

  template<typename F, typename ...Args>
  struct invokable : public std::integral_constant<
    bool, detail::invokable_imp<F, Args...>::value> {
  };

  // invoke_of

  template<typename F, typename ...Args>
  struct invoke_of : public detail::invoke_of_imp<invokable<F, Args...>::value,
						  F, Args...> {
  };

  template<typename F, typename ...Args>
  using invoke_of_t = typename invoke_of<F, Args...>::type;

  namespace detail {
    using boost::compressed_pair;
    using std::forward;
    using std::forward_as_tuple;
    using std::move;
    using std::unique_ptr;
    using std::type_info;

    // Suitable for passing as the deallocator for unique_ptr

    template <class Alloc>
    class allocator_destructor {
      typedef std::allocator_traits<Alloc> alloc_traits;
    public:
      typedef typename alloc_traits::pointer pointer;
      typedef typename alloc_traits::size_type size_type;
    private:
      Alloc& alloc;
      size_type s;
    public:
      allocator_destructor(Alloc& a, size_type s) noexcept
	: alloc(a), s(s) {}
      void operator()(pointer p) noexcept {
	alloc_traits::deallocate(alloc, p, s);
      }
    };

    template<typename Res, typename ...ArgTypes>
    struct maybe_derive_from_unary_function {
    };

    template<class Res, class Arg>
    struct maybe_derive_from_unary_function<Res(Arg)>
      : public std::unary_function<Arg, Res> {
    };

    template<class Res, class ...ArgTypes>
    struct maybe_derive_from_binary_function {
    };

    template<class Res, class Arg1, class Arg2>
    struct maybe_derive_from_binary_function<Res(Arg1, Arg2)>
      : public std::binary_function<Arg1, Arg2, Res> {
    };

    // Reimplement base without virtual function later

    template<class F>
    class base;

    template<class Res, class ...ArgTypes>
    class base<Res(ArgTypes...)> {
    public:
      base(const base&) = delete;
      base& operator=(const base&) = delete;
      base() {}
      virtual ~base() {}
      virtual base* clone() const = 0;
      virtual void clone(base*) const = 0;
      virtual void destroy() noexcept = 0;
      virtual void destroy_deallocate() noexcept = 0;
      virtual Res operator()(ArgTypes&& ...) = 0;
      virtual const void* target(const type_info& ti) const noexcept = 0;
      virtual const std::type_info& target_type() const noexcept = 0;
    };

    template<class FD, class Alloc, class FB>
    class func;

    template<class Fun, class Alloc, class Res, class ...ArgTypes>
    class func<Fun, Alloc, Res(ArgTypes...)>
      : public base<Res(ArgTypes...)> {
      compressed_pair<Fun, Alloc> f;
    public:
      explicit func(Fun&& _f) : f(move(_f)) {}

      explicit func(const Fun& _f, const Alloc& _a)
	: f(_f, _a) {}

      explicit func(const Fun& _f, Alloc&& _a)
	: f(move(_f), move(_a)) {}

      explicit func(Fun&& f, const Alloc& a)
	: f(forward_as_tuple(move(f)),
	    forward_as_tuple(a)) {}

      explicit func(Fun&& _f, Alloc&& _a)
	: f(move(_f), move(_a)) {}

      virtual base<Res(ArgTypes...)>* clone() const {
	typedef typename std::allocator_traits<Alloc>::template
	  rebind_alloc<func> Ap;
	Ap a(f.second());
	typedef allocator_destructor<Ap> Dp;
	unique_ptr<func, Dp> hold(a.allocate(1), Dp(a, 1));
	::new (hold.get()) func(f.first(), Alloc(a));
	return hold.release();
      }
      virtual void clone(base<Res(ArgTypes...)>* p) const {
	::new (p) func(f.first(), f.second());
      }
      virtual void destroy() noexcept {
	f.~compressed_pair<Fun, Alloc>();
      }
      virtual void destroy_deallocate() noexcept {
	typedef typename std::allocator_traits<Alloc>::template
	  rebind_alloc<func> Ap;
	Ap a(f.second());
	f.~compressed_pair<Fun, Alloc>();
	a.deallocate(this, 1);
      }
      virtual Res operator()(ArgTypes&& ...arg) {
	return invoke(f.first(), forward<ArgTypes>(arg)...);
      }
      virtual const void* target(const type_info& ti) const noexcept {
	if (ti == typeid(Fun))
	  return &f.first();
	return (const void*)0;
      }
      virtual const std::type_info& target_type() const noexcept {
	return typeid(Fun);
      }
    };


    template<bool>
    struct maybe_slist_hook {
    };

    template<>
    struct maybe_slist_hook<true> :
      boost::intrusive::slist_base_hook<
      boost::intrusive::link_mode<
      boost::intrusive::normal_link>> {
    };
  };

  template<typename F, size_t Reserved = 3 * sizeof(void*),
	   bool NoHeap = false, bool Link = false>
  class function; // undefined

  // The cohort::function class is template is, by default, equivalent
  // in all its behaviors to std::function. However there are two
  // optional template parameters giving the signature:

  // cohort::function<Res(ArgTypes...), Reserved = 3 * sizeof(void*),
  //                      NoHeap = false>

  // Res and ArgTypes are the return and argument types of the
  // function, as with std::function.

  // Reserved is the size of the static buffer into which small enough
  // function objects will be copied.

  // If true, NoHeap will disable allocation on the heap. (Fancy
  // that!)  by disabling constructors if the function argument is
  // larger than Reserved. It also disables copy constructors, move
  // constructors, assignment operators, and swaop between two
  // function objects unless they both have NoHeap true and the
  // reserved space of th function being assigned from is not larger
  // than the reserved space of the function being assigned to.

  // (This means we can rule out all allocations at compile
  // time. There are some copies/assignments that would not allocate,
  // but they can't be determined until runtime, and two out of the
  // two CohortFS developers surveyed said they preferred this result.)

  // If true, Link will add an normal_link slist_base_hook. It cannot
  // be a member hook, sorry, we're not StandardLayout enough for that
  // to work. I picked normal_link since auto_unlink is incompatible
  // with cache_last.

  template<typename Res, typename ...ArgTypes, size_t Reserved, bool NoHeap,
	   bool Link>
  class function<Res(ArgTypes...), Reserved, NoHeap, Link>
    : public detail::maybe_derive_from_unary_function<Res(ArgTypes...)>,
      public detail::maybe_derive_from_binary_function<Res(ArgTypes...)>,
      public detail::maybe_slist_hook<Link> {
    typedef detail::base<Res(ArgTypes...)> base;
    static constexpr size_t reserved =
      sizeof(detail::func<std::aligned_storage_t<Reserved, sizeof(void*)>,
	     std::allocator<std::aligned_storage_t<Reserved, sizeof(void*)>>,
	     Res(ArgTypes...)>);
    static constexpr bool no_heap = NoHeap;

    std::aligned_storage_t<reserved, sizeof(void*)> buf;
    size_t stored;
    base* b;

    template<typename Fun>
    static bool not_null(const Fun&) {
      return true;
    }
    template<typename R2, typename ...Ap>
    static bool not_null(R2 (*p)(Ap...)) {
      return p;
    }
    template <typename R2, typename Cp, typename Ap>
    static bool not_null(R2 (Cp::*p)(Ap)) {
      return p;
    }
    template <typename R2, typename Cp, typename Ap>
    static bool not_null(R2 (Cp::*p)(Ap...) const) {
      return p;
    }
    template <class R2, class Cp, class ...Ap>
    static bool not_null(R2 (Cp::*p)(Ap...) volatile) {
      return p;
    }
    template <class R2, class Cp, class ...Ap>
    static bool not_null(R2 (Cp::*p)(Ap...) const volatile) {
      return p;
    }
    template <class R2, class ...Ap>
    static bool not_null(const function<R2(Ap...)>& p) {
      return !!p;
    }

    template<typename Fun, bool = (!std::is_same<Fun, function>() &&
				   invokable<Fun&, ArgTypes...>())>
    struct callable;

    template<class Fun>
    struct callable<Fun, true> : std::integral_constant<
      bool, std::is_convertible<invoke_of_t<Fun&, ArgTypes...>, Res>::value> {
    };

    template<class Fun>
    struct callable<Fun, false> {
      static const bool value = false;
    };

public:
    typedef Res result_type;

    // construct/copy/destroy:
    function() noexcept : stored(0), b(0) {}
    function(std::nullptr_t) : stored(0), b(0) {}

    function(const function& f) : stored(0), b(0) {
      if (f.b == 0) {
	b = 0;
	assert(f.stored == 0);
      } else if (f.b == (base*) &f.buf) {
	b = (base*)(&buf);
	f.b->clone(b);
      } else {
	assert(!no_heap);
	b = f.b->clone();
      }
      stored = f.stored;
    }

    template<size_t _RS, bool _NH, bool _L>
    function(const std::enable_if_t<!no_heap ||
	     (_NH && _RS >= reserved),
	     function<Res(ArgTypes...), _RS, _NH, _L>>& f)
      : stored(0), b(0) {
      if (f.b == 0) {
	b = 0;
	assert(f.stored == 0);
      } else if (f.stored <= reserved) {
	b = (base*)(&buf);
	f.b->clone(b);
      } else {
	assert(!no_heap);
	b = f.b->clone();
      }
      stored = f.stored;
    }

    function(function&& f) noexcept : stored(0), b(0) {
      if (f.b == 0) {
	b = 0;
	assert(f.stored == 0);
      } else if (f.b == (base*) &f.buf) {
	b = (base*) &buf;
	f.b->clone(b);
	f.b->destroy();
      } else {
	assert(!no_heap);
	b = f.b;
      }
      stored = f.stored;
      f.b = 0;
      f.stored = 0;
    }

    template<size_t _RS, bool _NH, bool _L>
    function(std::enable_if_t<!no_heap ||
	     (_NH && _RS <= reserved),
	     function<Res(ArgTypes...), _RS, _NH, _L>>&& f) noexcept
	: stored(0), b(0) {
      if (f.b == 0) {
	b = 0;
	assert(f.stored == 0);
      } else if (f.stored <= reserved) {
	b = (base*) &buf;
	f.b->clone(b);
	f.b->destroy();
      } else if (f.b == (decltype(f.b)) &f.buf) {
	b = f.b->clone();
	f.b->destroy();
      } else {
	assert(!no_heap);
	b = f.b;
      }
      stored = f.stored;
      f.b = 0;
      f.stored = 0;
    }

    template<class Fun>
    function(Fun f,
	     std::enable_if_t<callable<Fun>() &&
	     !std::is_same<Fun, function>()>* = 0,
	     std::enable_if_t<!no_heap || reserved >= sizeof(Fun)>* = 0)
      : stored(0), b(0) {
      if (not_null(f)) {
	typedef detail::func<Fun, std::allocator<Fun>, Res(ArgTypes...)> FF;
	if (sizeof(FF) <= sizeof(buf) &&
	    std::is_copy_constructible<Fun>()) {
	  b = (base*) &buf;
	  ::new (b) FF(std::move(f));
	} else if (!no_heap) {
	  assert(!no_heap);
	  typedef std::allocator<FF> A;
	  A a;
	  typedef detail::allocator_destructor<A> D;
	  std::unique_ptr<base, D> hold(a.allocate(1), D(a, 1));
	  ::new (hold.get()) FF(std::move(f), std::allocator<Fun>(a));
	  b = hold.release();
	} else {
	  abort(); // Can't happen
	}
	stored = sizeof(FF);
      }
    }

    template<class Alloc, bool DB = true>
    function(std::enable_if_t<!no_heap && DB, std::allocator_arg_t>,
	     const Alloc&) noexcept : b(0) {}
    template<class Alloc, bool DB = true>
    function(std::enable_if_t<!no_heap && DB, std::allocator_arg_t>,
	     const Alloc&, std::nullptr_t) : b(0) {}
    template<class Alloc, bool DB = true>
    function(std::enable_if_t<!no_heap && DB, std::allocator_arg_t>,
	     const Alloc&, const function& f) {
      // This seems a bit fishy
      stored = f.stored;
      if (f.b == 0) {
	b = 0;
      } else if (f.b == (const base*)(&f.buf)) {
	b = (base*)(&buf);
	f.b->clone(b);
      } else {
	b = f.b->clone();
      }
    }
    template<class Alloc, bool DB = true>
    function(std::enable_if_t<!no_heap && DB, std::allocator_arg_t>,
	     const Alloc& a, function&& f) {
      stored = f.stored;
      if (f.b == 0) {
	f = 0;
      } else if (f.b == (base*) &f.buf) {
	b = (base*) &buf;
	f.b->clone(b);
    } else {
	b = f.b;
	f.b = 0;
      }
    }

    template<class Fun, class Alloc, bool DB = true>
    function(std::enable_if_t<!no_heap && DB, std::allocator_arg_t>,
	     const Alloc& a0, Fun f,
	     std::enable_if_t<callable<Fun>::value>* = 0) : b(0) {
      typedef std::allocator_traits<Alloc> alloc_traits;
      if (not_null(f)) {
	typedef detail::func<Fun, Alloc, Res(ArgTypes...)> FF;
	typedef typename alloc_traits::template rebind_alloc<FF> A;
	A a(a0);
	if (sizeof(FF) <= sizeof(buf) &&
	    std::is_copy_constructible<Fun>() &&
	    std::is_copy_constructible<A>()) {
	  b = (base*) &buf;
	  ::new (b) FF(std::move(f), Alloc(a));
	} else {
	  typedef detail::allocator_destructor<A> D;
	  std::unique_ptr<base, D> hold(a.allocate(1), D(a, 1));
	  ::new (hold.get()) FF(std::move(f), Alloc(a));
	  b = hold.release();
	}
	stored = sizeof(FF);
      }
    }

    function& operator=(const function& f) {
      function(f).swap(*this);
      return *this;
    }

    template<size_t _RS, bool _NH, bool _L>
    function& operator=(const std::enable_if_t<!no_heap ||
			(_NH && _RS <= reserved),
			function<Res(ArgTypes...), _RS, _NH, _L>>& f) {
      function(f).swap(*this);
      return *this;
    }

    function& operator=(function&& f) noexcept {
      if (b == (base*) &buf) {
	b->destroy();
      } else if (b) {
	assert(!no_heap);
	b->destroy_deallocate();
	b = 0;
      }

      stored = 0;
      if (f.b == 0) {
	b = 0;
	assert(f.stored == 0);
      } else if (f.b == (base*) &f.buf) {
	b = (base*) &buf;
	f.b->clone(b);
	f.b->destroy();
      } else {
	assert(!no_heap);
	b = f.b;
      }
      stored = f.stored;
      f.b = 0;
      f.stored = 0;

      return *this;
    }

    template<size_t _RS, bool _NH, bool _L>
    function& operator=(std::enable_if_t<!no_heap ||
			(_NH && _RS <= reserved),
			function<Res(ArgTypes...), _RS, _NH,
			_L>>&& f) noexcept {
      if (b == (base*) &buf) {
	b->destroy();
      } else if (b) {
	assert(!no_heap);
	b->destroy_deallocate();
	b = 0;
      }
      stored = 0;
      if (f.b == 0) {
	b = 0;
	assert(f.stored == 0);
      } else if (f.stored <= reserved) {
	b = (base*) &buf;
	f.b->clone(b);
	f.b->destroy();
      } else if (f.b == (decltype(f.b)*) f.buf) {
	b = (base*) &buf;
	f.b->clone(b);
	f.b->destroy_deallocate();
      } else {
	assert(!no_heap);
	swap(b, f.b);
      }
      stored = f.stored;
      f.b = 0;
      f.stored = 0;

      return *this;
    }

    function& operator=(std::nullptr_t) noexcept {
      if (b == (base*) &buf) {
	b->destroy();
      } else if (b) {
	assert(!no_heap);
	b->destroy_deallocate();
      }
      b = 0;
      return *this;
    }

    template<class Fun>
    std::enable_if_t<callable<std::decay_t<Fun>>() &&
      !std::is_same<std::remove_reference_t<Fun>, function>(),
      function&> operator=(Fun&& f) {
      function(std::forward<Fun>(f)).swap(*this);
      return *this;
    }

    ~function() {
      if (b == (base*) &buf) {
	b->destroy();
      } else if (b) {
	assert(!no_heap);
	b->destroy_deallocate();
      }
    }

    // function modifiers:
    template<size_t _RS, bool _NH, bool _L>
    auto swap(function<Res(ArgTypes...), _RS, _NH, _L>& f) noexcept ->
      std::enable_if_t<!no_heap || (_NH && _RS <= reserved), void> {
      if (!f && b) {
	f = std::move(*this);
	return;
      } else if (!b && f) {
	*this = std::move(f);
      } else if (!b && !f) {
	return;
      }
      if (stored <= f.reserved &&
	  f.stored <= reserved) {
	assert(f.b == (decltype(f.b)) &f.buf);
	assert(b == (base*) &buf);
	std::aligned_storage_t<reserved, sizeof(void*)> tempbuf;
	base* t = (base*) &tempbuf;
	b->clone(t);
	b->destroy();
	b = 0;
	f.b->clone((base*) &buf);
	f.b->destroy();
	f.b = 0;
	b = (base*) &buf;
	t->clone((base*) &f.buf);
	t->destroy();
	f.b = (base*) &f.buf;
	std::swap(stored, f.stored);
      } else if (stored > f.reserved &&
		 f.stored > reserved) {
	assert(!no_heap);
	std::swap(b, f.b);
	std::swap(stored, f.stored);
      } else {
	assert(!no_heap);
	std::remove_reference_t<decltype(f)> temp = std::move(f);
	f = std::move(*this);
	*this = std::move(temp);
      }
    }

    template<class Fun>
    void assign(Fun&& f,
		std::enable_if_t<!no_heap || reserved >= sizeof(Fun)>* = 0) {
      function(std::forward<Fun>(f)).swap(*this);
    }
    template<class Fun, class Alloc, bool DB = true>
    void assign(Fun&& f,
		const Alloc& a,
		std::enable_if_t<!no_heap || DB>* = 0) {
      function(std::allocator_arg, a, std::forward<Fun>(f)).swap(*this);
    }

    // function capacity:
    explicit operator bool() const noexcept {
      return b;
    }

    // deleted overloads close possible hole in the type system
    template<class R2, class... ArgTypes2>
    bool operator==(const function<R2(ArgTypes2...)>&) const = delete;
    template<class R2, class... ArgTypes2>
    bool operator!=(const function<R2(ArgTypes2...)>&) const = delete;
  public:
    // function invocation:
    Res operator()(ArgTypes...arg) const {
      if (b == 0)
	throw std::bad_function_call();
      return (*b)(std::forward<ArgTypes>(arg)...);
    }

    // function target access:
    const std::type_info& target_type() const noexcept {
      if (b == 0)
	return typeid(void);
      return b->target_type();
    }
    template <typename T>
    T* target() noexcept {
      if (b == 0)
	return 0;
      return (T*) b->target(typeid(T));
    }

    template<typename T>
    const T* target() const noexcept {
      if (b == 0)
	return 0;
      return (const T*) b->target(typeid(T));
    }
  };

  template<typename Res, typename ...ArgTypes, size_t Reserved, bool NoHeap,
	   bool Link>
  bool operator==(const function<Res(ArgTypes...), Reserved, NoHeap, Link>& f,
		  std::nullptr_t) noexcept {
    return !f;
  }

  template<typename Res, typename ...ArgTypes, size_t Reserved, bool NoHeap,
	   bool Link>
  bool operator==(
    std::nullptr_t,
    const function<Res(ArgTypes...), Reserved, NoHeap, Link>& f) noexcept {
    return !f;
  }

  template<typename Res, typename ...ArgTypes, size_t Reserved, bool NoHeap,
	   bool Link>
  bool operator!=(const function<Res(ArgTypes...), Reserved, NoHeap, Link>& f,
		  std::nullptr_t) noexcept {
    return f;
  }

  template<typename Res, typename ...ArgTypes, size_t Reserved, bool NoHeap,
	   bool Link>
  bool operator!=(std::nullptr_t,
		  const function<Res(ArgTypes...), Reserved,
		  NoHeap, Link>& f) noexcept {
    return f;
  }

  template<typename Res, typename ...ArgTypes, size_t R, bool N, bool L>
  void swap(function<Res(ArgTypes...), R, N, L>& x,
	    function<Res(ArgTypes...), R, N, L>& y) noexcept {
    return x.swap(y);
  }
};

#endif // COMMON_COHORT_FUNCTION_H
