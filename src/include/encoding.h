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
#ifndef CEPH_ENCODING_H
#define CEPH_ENCODING_H

#include "include/int_types.h"

#include "include/memory.h"

#include "byteorder.h"
#include "buffer.h"
#include "assert.h"

using namespace ceph;

/*
 * Notes on feature encoding:
 *
 * - The default encode() methods have a features argument with a default parameter
 *   (which goes to zero).
 * - Normal classes will use WRITE_CLASS_ENCODER, with that features=0 default.
 * - Classes that _require_ features will use WRITE_CLASS_ENCODER_FEATURES, which
 *   does not define the default.  Any caller must explicitly pass it in.
 * - STL container macros have two encode variants: one with a features arg, and one
 *   without.
 *
 * The result:
 * - A feature encode() method will fail to compile if a value is not
 *   passed in.
 * - The feature varianet of the STL templates will be used when the feature arg is
 *   provided.  It will be passed through to any template arg types, but it will be
 *   ignored when not needed.
 */

template<class T>
inline void encode_raw(const T& t, bufferlist& bl)
{
  bl.append((char*)&t, sizeof(t));
}
template<class T>
inline void encode_raw(const T& t, bufferlist::safe_appender& ap)
{
  ap.append_v(t);
}
template<class T>
inline void encode_raw(const T& t, bufferlist::unsafe_appender& ap)
{
  ap.append_v(t);
}
template<class T>
inline void decode_raw(T& t, bufferlist::iterator &p)
{
  p.copy(sizeof(t), (char*)&t);
}

/* Advanced encoding
 *
 * Recursively calling encode/decode to serialize a compound structure has the
 * advantage of being pleasantly modular and easy to work with.  However, it
 * means that we never know how much any particular ::encode call is going to
 * encode.  Thus, we are forced to use bufferlist in a highly conservative way
 * causing bounds checks and potentially allocations on every primitive.  This
 * isn't great, at times, we know statically or at a high level dynamically
 * (the encoding size of vector<int> isn't know statically, but it *is* known
 * without examining any actual elements).  Thus, we'd like some additional
 * static decorators.  Define a type function:
 */
template <typename T, typename = void> struct enc_dec_traits {
  static const bool supported = false;
  static const bool feature = false;
  static const bool bounded_size = false;
  static const size_t max_size = 0;
};
/* To enable support for adavanced encoding/decoding for type T,
 * define enc_dec_traits<T> with supported = true.
 * feature = true requires features to be passed in for encode/decode.
 *
 * Required
 * size_t estimate(const T &v);
 * template <typename App> void encode(const T &v, App &app, uint64_t features[=0]);
 * void decode(const T &v, bufferlist::iterator &bp, uint64_t features[=0]);
 *
 * Optional:
 * static size_t max_size; // must be an upper bound on any encoding
 */

/* First, define good old encode/decode in terms of enc_dec_traits */
template<typename type, typename traits=enc_dec_traits<type> >
inline typename std::enable_if<traits::supported && !traits::feature>::type
encode(const type &v, bufferlist &bl, uint64_t features=0) {
  size_t size = traits::estimate(v);
  bufferlist::unsafe_appender app(&bl, size);
  traits::encode(v, app);
}

template<typename type, typename traits=enc_dec_traits<type> >
inline typename std::enable_if<traits::supported && traits::feature>::type
encode(const type &v, bufferlist &bl, uint64_t features) {
  size_t size = traits::estimate(v, features);
  bufferlist::unsafe_appender app(&bl, size);
  traits::encode(v, app, features);
}

template<typename type, typename traits=enc_dec_traits<type> >
inline typename std::enable_if<traits::supported && !traits::feature>::type
encode(const type &v, bufferlist::safe_appender &app, uint64_t features=0) {
  traits::encode(v, app);
}

template<typename type, typename traits=enc_dec_traits<type> >
inline typename std::enable_if<traits::supported && traits::feature>::type
encode(const type &v, bufferlist::safe_appender &app, uint64_t features) {
  traits::encode(v, app, features);
}

template<typename type, typename traits=enc_dec_traits<type> >
inline typename std::enable_if<traits::supported && !traits::feature>::type
encode(const type &v, bufferlist::unsafe_appender &app, uint64_t features=0) {
  traits::encode(v, app);
}

template<typename type, typename traits=enc_dec_traits<type> >
inline typename std::enable_if<traits::supported && traits::feature>::type
encode(const type &v, bufferlist::unsafe_appender &app, uint64_t features) {
  size_t size = traits::estimate(v, features);
  traits::encode(v, app, features);
}

template<typename type, typename traits=enc_dec_traits<type> >
inline typename std::enable_if<traits::supported && !traits::feature>::type
decode(type &v, bufferlist::iterator &bl, uint64_t features=0) {
  traits::decode(v, bl);
}

template<typename type, typename traits=enc_dec_traits<type> >
inline typename std::enable_if<traits::supported && traits::feature>::type
decode(type &v, bufferlist::iterator &bl, uint64_t features) {
  traits::decode(v, bl, features);
}

/* Next, define the new-style encdec -- only available for new-style */
template<typename type, typename app, typename traits>
void encdec() {
}

template<typename type, typename app, typename traits=enc_dec_traits<type> >
inline typename std::enable_if<traits::supported && !traits::feature>::type
encdec(const type &v, app &a, uint64_t features=0) {
  traits::encode(v, a);
}

template<typename type, typename app, typename traits=enc_dec_traits<type> >
inline typename std::enable_if<traits::supported && traits::feature>::type
encdec(const type &v, app &a, uint64_t features) {
  traits::encode(v, a, features);
}

template<typename type, typename traits=enc_dec_traits<type> >
inline typename std::enable_if<traits::supported && !traits::feature>::type
encdec(
  type &v, bufferlist::iterator &bl, uint64_t features=0) {
  traits::decode(v, bl);
}

template<typename type, typename traits=enc_dec_traits<type> >
inline typename std::enable_if<traits::supported && traits::feature>::type
encdec(
  type &v, bufferlist::iterator &bl, uint64_t features) {
  traits::decode(v, bl, features);
}

template<typename type, typename traits=enc_dec_traits<type> >
inline typename std::enable_if<traits::supported && !traits::feature>::type
encdec(
  const type &v, size_t &cnt, uint64_t features=0) {
  cnt += traits::estimate(v);
}

template<typename type, typename traits=enc_dec_traits<type> >
inline typename std::enable_if<traits::supported && traits::feature>::type
encdec(
  const type &v, size_t &cnt, uint64_t features) {
  cnt += traits::estimate(v, features);
}

/* Finally, define an instance for enc_dec_traits which automagically
 * generates an encdec typefunction for anything with a templated
 * encdec method which takes the right arguments.
 */
template<typename T>
struct true_if_valid { static const bool value = true; };

template<typename type>
struct enc_dec_traits<
  type,
  typename std::enable_if<
    true_if_valid<
      decltype(std::declval<type>().encdec(
		 std::declval<size_t&>()))
      >::value &&
    true_if_valid<
      decltype(std::declval<type>().encdec(
	std::declval<bufferlist::iterator&>()))
      >::value &&
    true_if_valid<
      decltype(std::declval<type>().encdec(
	std::declval<bufferlist::safe_appender&>()))
      >::value &&
    true_if_valid<
      decltype(std::declval<type>().encdec(
	std::declval<bufferlist::unsafe_appender&>()))
      >::value
    >::type
  > {
  static const bool supported = true;
  static const bool feature = false;
  static const bool bounded_size = false;
  static const size_t max_size = 0;

  template <typename App> static void encode(
    const type &v, App &ap, uint64_t features = 0) {
    const_cast<type &>(v).encdec(ap); /* TODO: figure out, this is not great */
  }
  static void decode(
    type &v, bufferlist::iterator &bl, uint64_t features = 0) {
    v.encdec(bl);
  }
  static size_t estimate(const type &v, uint64_t features = 0) {
    size_t ret = 0;
    const_cast<type &>(v).encdec(ret);
    return ret;
  }
};

template<typename type>
struct enc_dec_traits<
  type,
  typename std::enable_if<
    true_if_valid<
      decltype(std::declval<type>().encdec(
        std::declval<size_t&>(),
	std::declval<uint64_t>()))
      >::value &&
    true_if_valid<
      decltype(std::declval<type>().encdec(
        std::declval<bufferlist::iterator&>(),
	std::declval<uint64_t>()))
      >::value &&
    true_if_valid<
      decltype(std::declval<type>().encdec(
        std::declval<bufferlist::safe_appender&>(),
	std::declval<uint64_t>()))
      >::value &&
    true_if_valid<
      decltype(std::declval<type>().encdec(
        std::declval<bufferlist::unsafe_appender&>(),
	std::declval<uint64_t>()))
      >::value
    >::type
  > {
  static const bool supported = true;
  static const bool feature = true;
  static const bool bounded_size = false;
  static const size_t max_size = 0;

  template <typename App> static void encode(
    const type &v, App &ap, uint64_t features) {
    const_cast<type &>(v).encdec(ap, features);
  }
  static void decode(
    type &v, bufferlist::iterator &bl, uint64_t features) {
    v.encdec(bl, features);
  }
  static size_t estimate(const type &v, uint64_t features) {
    size_t ret = 0;
    const_cast<type &>(v).encdec(ret, features);
    return ret;
  }
};

// --------------------------------------
// base types

#define WRITE_RAW_ENCODER(type)						\
  template<>                                                            \
  struct enc_dec_traits<type> {                                         \
    static const bool supported = true;                                 \
    static const bool feature = false;                                  \
    static const bool bounded_size = true;                              \
    static const size_t max_size = sizeof(type);                        \
    template <typename App> static void encode(                         \
      const type &v, App &app, uint64_t features = 0) {                 \
      encode_raw(v, app);                                               \
    }                                                                   \
    static void decode(                                                 \
      type &v, bufferlist::iterator &bl, uint64_t features = 0) {       \
      decode_raw(v, bl);                                                \
    }                                                                   \
    static size_t estimate(const type &v, uint64_t feature = 0) {       \
      return max_size;                                                  \
    }							                \
  };                                                                    \

WRITE_RAW_ENCODER(__u8)
#ifndef _CHAR_IS_SIGNED
WRITE_RAW_ENCODER(__s8)
#endif
WRITE_RAW_ENCODER(char)
WRITE_RAW_ENCODER(ceph_le64)
WRITE_RAW_ENCODER(ceph_le32)
WRITE_RAW_ENCODER(ceph_le16)

// FIXME: we need to choose some portable floating point encoding here
WRITE_RAW_ENCODER(float)
WRITE_RAW_ENCODER(double)

template <>
struct enc_dec_traits<bool> {
  static const bool supported = true;
  static const bool feature = false;
  static const bool bounded_size = true;
  static const size_t max_size = 1;
  template <typename App> static void encode(
    const bool &b, App &app, uint64_t features = 0) {
    __u8 vv = b;
    encode_raw(vv, app);
  }
  static void decode(
    bool &b, bufferlist::iterator &p, uint64_t features = 0) {
    __u8 vv;
    decode_raw(vv, p);
    b = vv;
  }
  static size_t estimate(
    const bool &b, uint64_t features = 0) {
    return 1;
  }
};

// -----------------------------------
// int types

#define WRITE_INTTYPE_ENCODER(type, etype)				\
  template<>                                                            \
  struct enc_dec_traits<type> {                                         \
    static const bool supported = true;                                 \
    static const bool feature = false;                                  \
    static const bool bounded_size = true;                              \
    static const size_t max_size = sizeof(ceph_##etype);                \
    template <typename App> static void encode(                         \
      const type &v, App &app, uint64_t features = 0) {                 \
      ceph_##etype e;                                                   \
      e = v;                                                            \
      encode_raw(e, app);                                               \
    }                                                                   \
    static void decode(                                                 \
      type &v, bufferlist::iterator &bl, uint64_t features = 0) {       \
      ceph_##etype e;                                                   \
      decode_raw(e, bl);                                                \
      v = e;                                                            \
    }                                                                   \
    static size_t estimate(const type &v) { return max_size; }          \
  };                                                                    \

WRITE_INTTYPE_ENCODER(uint64_t, le64)
WRITE_INTTYPE_ENCODER(int64_t, le64)
WRITE_INTTYPE_ENCODER(uint32_t, le32)
WRITE_INTTYPE_ENCODER(int32_t, le32)
WRITE_INTTYPE_ENCODER(uint16_t, le16)
WRITE_INTTYPE_ENCODER(int16_t, le16)

#ifdef ENCODE_DUMP
# include <stdio.h>
# include <sys/types.h>
# include <sys/stat.h>
# include <fcntl.h>

# define ENCODE_STR(x) #x
# define ENCODE_STRINGIFY(x) ENCODE_STR(x)

# define ENCODE_DUMP_PRE()			\
  unsigned pre_off = bl.length()

// NOTE: This is almost an exponential backoff, but because we count
// bits we get a better sample of things we encode later on.
# define ENCODE_DUMP_POST(cl)						\
  do {									\
    static int i = 0;							\
    i++;								\
    int bits = 0;							\
    for (unsigned t = i; t; bits++)					\
      t &= t - 1;							\
    if (bits > 2)							\
      break;								\
    char fn[200];							\
    snprintf(fn, sizeof(fn), ENCODE_STRINGIFY(ENCODE_DUMP) "/%s__%d.%x", #cl, getpid(), i++); \
    int fd = ::open(fn, O_WRONLY|O_TRUNC|O_CREAT, 0644);		\
    if (fd >= 0) {							\
      bufferlist sub;							\
      sub.substr_of(bl, pre_off, bl.length() - pre_off);		\
      sub.write_fd(fd);							\
      ::close(fd);							\
    }									\
  } while (0)
#else
# define ENCODE_DUMP_PRE()
# define ENCODE_DUMP_POST(cl)
#endif

#define WRITE_CLASS_ENCODER(cl)						\
  inline void encode(const cl &c, bufferlist &bl, uint64_t features=0) { \
    ENCODE_DUMP_PRE(); c.encode(bl); ENCODE_DUMP_POST(cl); }		\
  inline void decode(cl &c, bufferlist::iterator &p) { c.decode(p); }

#define WRITE_CLASS_MEMBER_ENCODER(cl)					\
  inline void encode(const cl &c, bufferlist &bl) const {		\
    ENCODE_DUMP_PRE(); c.encode(bl); ENCODE_DUMP_POST(cl); }		\
  inline void decode(cl &c, bufferlist::iterator &p) { c.decode(p); }

#define WRITE_CLASS_ENCODER_FEATURES(cl)				\
  inline void encode(const cl &c, bufferlist &bl, uint64_t features) {	\
    ENCODE_DUMP_PRE(); c.encode(bl, features); ENCODE_DUMP_POST(cl); }	\
  inline void decode(cl &c, bufferlist::iterator &p) { c.decode(p); }

#define WRITE_CLASS_ENCODER_OPTIONAL_FEATURES(cl)				\
  inline void encode(const cl &c, bufferlist &bl, uint64_t features = 0) {	\
    ENCODE_DUMP_PRE(); c.encode(bl, features); ENCODE_DUMP_POST(cl); }	\
  inline void decode(cl &c, bufferlist::iterator &p) { c.decode(p); }


// string
template<>
struct enc_dec_traits<std::string> {
  static const bool supported = true;
  static const bool feature = false;
  static const bool bounded_size = false;
  static const size_t max_size = 0;

  template <typename App> static void encode(
    const std::string &str,
    App &app,
    uint64_t features = 0) {
    __u32 len = str.length();
    ::encode(len, app);
    if (len)
      app.append(str.data(), len);
  }

  static void decode(
    std::string &str,
    bufferlist::iterator &p,
    uint64_t features = 0) {
    __u32 len;
    ::decode(len, p);
    str.clear();
    p.copy(len, str);
  }

  static size_t estimate(
    const std::string &str,
    uint64_t features = 0) {
    return str.length();
  }
};

inline void encode_nohead(const std::string& s, bufferlist& bl)
{
  bl.append(s.data(), s.length());
}
inline void decode_nohead(int len, std::string& s, bufferlist::iterator& p)
{
  s.clear();
  p.copy(len, s);
}

// const char* (encode only, string compatible)
inline void encode(const char *s, bufferlist& bl) 
{
  __u32 len = strlen(s);
  encode(len, bl);
  if (len)
    bl.append(s, len);
}


// array
template<class A>
inline void encode_array_nohead(const A a[], int n, bufferlist &bl)
{
  for (int i=0; i<n; i++)
    encode(a[i], bl);
}
template<class A>
inline void decode_array_nohead(A a[], int n, bufferlist::iterator &p)
{
  for (int i=0; i<n; i++)
    decode(a[i], p);
}



// -----------------------------
// buffers

// bufferptr (encapsulated)
inline void encode(const buffer::ptr& bp, bufferlist& bl) 
{
  __u32 len = bp.length();
  encode(len, bl);
  if (len)
    bl.append(bp);
}
inline void decode(buffer::ptr& bp, bufferlist::iterator& p)
{
  __u32 len;
  decode(len, p);

  bufferlist s;
  p.copy(len, s);

  if (len) {
    if (s.get_num_buffers() == 1)
      bp = s.front();
    else
      bp = buffer::copy(s.c_str(), s.length());
  }
}

// bufferlist (encapsulated)
inline void encode(const bufferlist& s, bufferlist& bl) 
{
  __u32 len = s.length();
  encode(len, bl);
  bl.append(s);
}
inline void encode_destructively(bufferlist& s, bufferlist& bl) 
{
  __u32 len = s.length();
  encode(len, bl);
  bl.claim_append(s);
}
inline void decode(bufferlist& s, bufferlist::iterator& p)
{
  __u32 len;
  decode(len, p);
  s.clear();
  p.copy(len, s);
}

inline void encode_nohead(const bufferlist& s, bufferlist& bl) 
{
  bl.append(s);
}
inline void decode_nohead(int len, bufferlist& s, bufferlist::iterator& p)
{
  s.clear();
  p.copy(len, s);
}


// full bl decoder
template<class T>
inline void decode(T &o, bufferlist& bl)
{
  bufferlist::iterator p = bl.begin();
  decode(o, p);
  assert(p.end());
}


// -----------------------------
// STL container types

#include <set>
#include <map>
#include <deque>
#include <vector>
#include <string>
#include <boost/optional/optional_io.hpp>
#include <boost/tuple/tuple.hpp>

#ifndef _BACKWARD_BACKWARD_WARNING_H
#define _BACKWARD_BACKWARD_WARNING_H   // make gcc 4.3 shut up about hash_*
#endif
#include "include/unordered_map.h"
#include "include/unordered_set.h"


// boost optional
template<typename T>
inline void encode(const boost::optional<T> &p, bufferlist &bl)
{
  __u8 present = static_cast<bool>(p);
  ::encode(present, bl);
  if (p)
    encode(p.get(), bl);
}

#pragma GCC diagnostic ignored "-Wpragmas"
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wuninitialized"
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
template<typename T>
inline void decode(boost::optional<T> &p, bufferlist::iterator &bp)
{
  __u8 present;
  ::decode(present, bp);
  if (present) {
    T t;
    p = t;
    decode(p.get(), bp);
  }
}
#pragma GCC diagnostic pop
#pragma GCC diagnostic warning "-Wpragmas"

//triple tuple
template<class A, class B, class C>
inline void encode(const boost::tuple<A, B, C> &t, bufferlist& bl)
{
  encode(boost::get<0>(t), bl);
  encode(boost::get<1>(t), bl);
  encode(boost::get<2>(t), bl);
}
template<class A, class B, class C>
inline void decode(boost::tuple<A, B, C> &t, bufferlist::iterator &bp)
{
  decode(boost::get<0>(t), bp);
  decode(boost::get<1>(t), bp);
  decode(boost::get<2>(t), bp);
}

// pair

/* Restrict the basic encode/decode definitions to pairs where 
 * either type lacks an enc_dec_traits specialization */
template<class A, class B>
inline
typename std::enable_if<
  !enc_dec_traits<A>::supported ||
  !enc_dec_traits<B>::supported>::type
encode(const std::pair<A,B> &p, bufferlist &bl, uint64_t features)
{
  encode(p.first, bl, features);
  encode(p.second, bl, features);
}
template<class A, class B>
inline
typename std::enable_if<
  !enc_dec_traits<A>::supported ||
  !enc_dec_traits<B>::supported>::type
encode(const std::pair<A,B> &p, bufferlist &bl)
{
  encode(p.first, bl);
  encode(p.second, bl);
}
template<class A, class B>
inline
typename std::enable_if<
  !enc_dec_traits<A>::supported ||
  !enc_dec_traits<B>::supported>::type
decode(std::pair<A,B> &pa, bufferlist::iterator &p)
{
  decode(pa.first, p);
  decode(pa.second, p);
}

/* To support types with advanced encoding, if both support enc_dec_traits,
 * we specialize an instance of enc_dec_traits (feature if either requires
 * it) */
template<typename A, typename B>
struct enc_dec_traits<
  std::pair<A, B>,
  typename std::enable_if<
    enc_dec_traits<A>::supported && enc_dec_traits<B>::supported &&
    (enc_dec_traits<A>::feature || enc_dec_traits<B>::feature)
  >::type> {
  const static bool supported = true;
  const static bool feature = true;
  const static bool bounded_size =
    enc_dec_traits<A>::bounded_size && enc_dec_traits<B>::bounded_size;
  const static size_t max_size =
    enc_dec_traits<A>::max_size + enc_dec_traits<B>::max_size;

  static size_t estimate(
    const std::pair<A, B> &v, uint64_t features) {
    return enc_dec_traits<A>::estimate(v.first, features) +
      enc_dec_traits<B>::estimate(v.second, features);
  }

  template <typename App> static void encode(
    const std::pair<A, B> &v, App &app, uint64_t features) {
    enc_dec_traits<A>::encode(v.first, app);
    enc_dec_traits<B>::encode(v.second, app);
  }
  static void decode(
    std::pair<A, B> &v, bufferlist::iterator &bl, uint64_t features) {
    enc_dec_traits<A>::decode(v.first, bl);
    enc_dec_traits<B>::decode(v.second, bl);
  }
};

template<typename A, typename B>
struct enc_dec_traits<
  std::pair<A, B>,
  typename std::enable_if<
    enc_dec_traits<A>::supported && enc_dec_traits<B>::supported &&
    !enc_dec_traits<A>::feature && !enc_dec_traits<B>::feature
  >::type> {
  const static bool supported = true;
  const static bool feature = false;
  const static bool bounded_size =
    enc_dec_traits<A>::bounded_size && enc_dec_traits<B>::bounded_size;
  const static size_t max_size =
    enc_dec_traits<A>::max_size + enc_dec_traits<B>::max_size;

  static size_t estimate(
    const std::pair<A, B> &v, uint64_t features = 0) {
    return enc_dec_traits<A>::estimate(v.first) +
      enc_dec_traits<B>::estimate(v.second);
  }

  template <typename App> static void encode(
    const std::pair<A, B> &v, App &app, uint64_t features = 0) {
    enc_dec_traits<A>::encode(v.first, app);
    enc_dec_traits<B>::encode(v.second, app);
  }
  static void decode(
    std::pair<A, B> &v, bufferlist::iterator &bl, uint64_t features = 0) {
    enc_dec_traits<A>::decode(v.first, bl);
    enc_dec_traits<B>::decode(v.second, bl);
  }
};

// list
template<class T>
inline void encode(const std::list<T>& ls, bufferlist& bl)
{
  __u32 n = (__u32)(ls.size());  // c++11 std::list::size() is O(1)
  encode(n, bl);
  for (typename std::list<T>::const_iterator p = ls.begin(); p != ls.end(); ++p)
    encode(*p, bl);
}
template<class T>
inline void encode(const std::list<T>& ls, bufferlist& bl, uint64_t features)
{
  // should i pre- or post- count?
  if (!ls.empty()) {
    unsigned pos = bl.length();
    unsigned n = 0;
    encode(n, bl);
    for (typename std::list<T>::const_iterator p = ls.begin(); p != ls.end(); ++p) {
      n++;
      encode(*p, bl, features);
    }
    ceph_le32 en;
    en = n;
    bl.copy_in(pos, sizeof(en), (char*)&en);
  } else {
    __u32 n = (__u32)(ls.size());    // FIXME: this is slow on a list.
    encode(n, bl);
    for (typename std::list<T>::const_iterator p = ls.begin(); p != ls.end(); ++p)
      encode(*p, bl, features);
  }
}
template<class T>
inline void decode(std::list<T>& ls, bufferlist::iterator& p)
{
  __u32 n;
  decode(n, p);
  ls.clear();
  while (n--) {
    T v;
    decode(v, p);
    ls.push_back(v);
  }
}

template<class T>
inline void encode(const std::list<ceph::shared_ptr<T> >& ls, bufferlist& bl)
{
  __u32 n = (__u32)(ls.size());  // c++11 std::list::size() is O(1)
  encode(n, bl);
  for (typename std::list<ceph::shared_ptr<T> >::const_iterator p = ls.begin(); p != ls.end(); ++p)
    encode(**p, bl);
}
template<class T>
inline void encode(const std::list<ceph::shared_ptr<T> >& ls, bufferlist& bl, uint64_t features)
{
  __u32 n = (__u32)(ls.size());  // c++11 std::list::size() is O(1)
  encode(n, bl);
  for (typename std::list<ceph::shared_ptr<T> >::const_iterator p = ls.begin(); p != ls.end(); ++p)
    encode(**p, bl, features);
}
template<class T>
inline void decode(std::list<ceph::shared_ptr<T> >& ls, bufferlist::iterator& p)
{
  __u32 n;
  decode(n, p);
  ls.clear();
  while (n--) {
    ceph::shared_ptr<T> v(std::make_shared<T>());
    decode(*v, p);
    ls.push_back(v);
  }
}

// set
template<class T>
inline void encode(const std::set<T>& s, bufferlist& bl)
{
  __u32 n = (__u32)(s.size());
  encode(n, bl);
  for (typename std::set<T>::const_iterator p = s.begin(); p != s.end(); ++p)
    encode(*p, bl);
}
template<class T>
inline void decode(std::set<T>& s, bufferlist::iterator& p)
{
  __u32 n;
  decode(n, p);
  s.clear();
  while (n--) {
    T v;
    decode(v, p);
    s.insert(v);
  }
}

template<class T, class C>
inline void encode(const std::set<T, C>& s, bufferlist& bl)
{
  __u32 n = (__u32)(s.size());
  encode(n, bl);
  for (typename std::set<T, C>::const_iterator p = s.begin(); p != s.end(); ++p)
    encode(*p, bl);
}
template<class T, class C>
inline void decode(std::set<T, C>& s, bufferlist::iterator& p)
{
  __u32 n;
  decode(n, p);
  s.clear();
  while (n--) {
    T v;
    decode(v, p);
    s.insert(v);
  }
}

template<class T>
inline void encode_nohead(const std::set<T>& s, bufferlist& bl)
{
  for (typename std::set<T>::const_iterator p = s.begin(); p != s.end(); ++p)
    encode(*p, bl);
}
template<class T>
inline void decode_nohead(int len, std::set<T>& s, bufferlist::iterator& p)
{
  for (int i=0; i<len; i++) {
    T v;
    decode(v, p);
    s.insert(v);
  }
}

// multiset
template<class T>
inline void encode(const std::multiset<T>& s, bufferlist& bl)
{
  __u32 n = (__u32)(s.size());
  encode(n, bl);
  for (typename std::multiset<T>::const_iterator p = s.begin(); p != s.end(); ++p)
    encode(*p, bl);
}
template<class T>
inline void decode(std::multiset<T>& s, bufferlist::iterator& p)
{
  __u32 n;
  decode(n, p);
  s.clear();
  while (n--) {
    T v;
    decode(v, p);
    s.insert(v);
  }
}

// vector (pointers)
/*template<class T>
inline void encode(const std::vector<T*>& v, bufferlist& bl)
{
  __u32 n = v.size();
  encode(n, bl);
  for (typename std::vector<T*>::const_iterator p = v.begin(); p != v.end(); ++p)
    encode(**p, bl);
}
template<class T>
inline void decode(std::vector<T*>& v, bufferlist::iterator& p)
{
  __u32 n;
  decode(n, p);
  v.resize(n);
  for (__u32 i=0; i<n; i++) 
    v[i] = new T(p);
}
*/
// vector
template<class T>
inline
typename std::enable_if<!enc_dec_traits<T>::supported>::type
encode(const std::vector<T>& v, bufferlist& bl, uint64_t features)
{
  __u32 n = (__u32)(v.size());
  encode(n, bl);
  for (typename std::vector<T>::const_iterator p = v.begin(); p != v.end(); ++p)
    encode(*p, bl, features);
}
template<class T>
inline
typename std::enable_if<!enc_dec_traits<T>::supported>::type
encode(const std::vector<T>& v, bufferlist& bl)
{
  __u32 n = (__u32)(v.size());
  encode(n, bl);
  for (typename std::vector<T>::const_iterator p = v.begin(); p != v.end(); ++p)
    encode(*p, bl);
}
template<class T>
inline
typename std::enable_if<!enc_dec_traits<T>::supported>::type
decode(std::vector<T>& v, bufferlist::iterator& p)
{
  __u32 n;
  decode(n, p);
  v.resize(n);
  for (__u32 i=0; i<n; i++) 
    decode(v[i], p);
}

template<typename T>
struct enc_dec_traits<
  std::vector<T>,
  typename std::enable_if<
    enc_dec_traits<T>::supported && !enc_dec_traits<T>::feature>::type> {
  const static bool supported = true;
  const static bool feature = false;
  const static bool bounded_size = false;

  template <typename U=T> static typename std::enable_if<
    enc_dec_traits<U>::bounded_size, size_t
    >::type estimate(const std::vector<U> &u, uint64_t features = 0) {
    return 4 + enc_dec_traits<U>::max_size + u.size();
  }

  template <typename U=T> static typename std::enable_if<
    !enc_dec_traits<U>::bounded_size, size_t
    >::type estimate(const std::vector<U> &u, uint64_t features = 0) {
    size_t ret = 4;
    for (auto &&i: u) {
      ret += enc_dec_traits<T>::estimate(i);
    }
    return ret;
  }

  template <typename App> static void encode(
    const std::vector<T> &v, App &app, uint64_t features = 0) {
    __u32 n = (__u32)(v.size());
    ::encode(n, app);
    for (auto &&i: v) {
      enc_dec_traits<T>::encode(i, app);
    }
  }

  static void decode(
    std::vector<T> &v, bufferlist::iterator &p, uint64_t features = 0) {
    __u32 n;
    ::decode(n, p);
    v.resize(n);
    for (__u32 i=0; i<n; i++)  {
      enc_dec_traits<T>::decode(v[i], p);
    }
  }
};

template<typename T>
struct enc_dec_traits<
  std::vector<T>,
  typename std::enable_if<
    enc_dec_traits<T>::supported && enc_dec_traits<T>::feature>::type> {
  const static bool supported = true;
  const static bool feature = true;
  const static bool bounded_size = false;

  template <typename U=T> static typename std::enable_if<
    enc_dec_traits<U>::bounded_size, size_t
    >::type estimate(const std::vector<U> &u, uint64_t features) {
    return 4 + enc_dec_traits<U>::max_size + u.size();
  }

  template <typename U=T> static typename std::enable_if<
    !enc_dec_traits<U>::bounded_size, size_t
    >::type estimate(const std::vector<U> &u, uint64_t features) {
    size_t ret = 4;
    for (auto &&i: u) {
      ret += enc_dec_traits<T>::estimate(i, features);
    }
    return ret;
  }

  template <typename App> static void encode(
    const std::vector<T> &v, App &app, uint64_t features) {
    __u32 n = (__u32)(v.size());
    ::encode(n, app);
    for (auto &&i: v) {
      enc_dec_traits<T>::encode(i, app, features);
    }
  }

  static void decode(
    std::vector<T> &v, bufferlist::iterator &p, uint64_t features) {
    __u32 n;
    ::decode(n, p);
    v.resize(n);
    for (__u32 i=0; i<n; i++)  {
      enc_dec_traits<T>::decode(v[i], p, features);
    }
  }
};

template<class T>
inline void encode_nohead(const std::vector<T>& v, bufferlist& bl)
{
  for (typename std::vector<T>::const_iterator p = v.begin(); p != v.end(); ++p)
    encode(*p, bl);
}
template<class T>
inline void decode_nohead(int len, std::vector<T>& v, bufferlist::iterator& p)
{
  v.resize(len);
  for (__u32 i=0; i<v.size(); i++) 
    decode(v[i], p);
}

// vector (shared_ptr)
template<class T>
inline void encode(const std::vector<ceph::shared_ptr<T> >& v, bufferlist& bl,
		   uint64_t features)
{
  __u32 n = (__u32)(v.size());
  encode(n, bl);
  for (typename std::vector<ceph::shared_ptr<T> >::const_iterator p = v.begin(); p != v.end(); ++p)
    if (*p)
      encode(**p, bl, features);
    else
      encode(T(), bl, features);
}
template<class T>
inline void encode(const std::vector<ceph::shared_ptr<T> >& v, bufferlist& bl)
{
  __u32 n = (__u32)(v.size());
  encode(n, bl);
  for (typename std::vector<ceph::shared_ptr<T> >::const_iterator p = v.begin(); p != v.end(); ++p)
    if (*p)
      encode(**p, bl);
    else
      encode(T(), bl);
}
template<class T>
inline void decode(std::vector<ceph::shared_ptr<T> >& v, bufferlist::iterator& p)
{
  __u32 n;
  decode(n, p);
  v.resize(n);
  for (__u32 i=0; i<n; i++) {
    v[i] = std::make_shared<T>();
    decode(*v[i], p);
  }
}

// map (pointers)
/*
template<class T, class U>
inline void encode(const std::map<T,U*>& m, bufferlist& bl)
{
  __u32 n = m.size();
  encode(n, bl);
  for (typename std::map<T,U*>::const_iterator p = m.begin(); p != m.end(); ++p) {
    encode(p->first, bl);
    encode(*p->second, bl);
  }
}
template<class T, class U>
inline void decode(std::map<T,U*>& m, bufferlist::iterator& p)
{
  __u32 n;
  decode(n, p);
  m.clear();
  while (n--) {
    T k;
    decode(k, p);
    m[k] = new U(p);
  }
  }*/

// map
template<class T, class U>
inline void encode(const std::map<T,U>& m, bufferlist& bl)
{
  __u32 n = (__u32)(m.size());
  encode(n, bl);
  for (typename std::map<T,U>::const_iterator p = m.begin(); p != m.end(); ++p) {
    encode(p->first, bl);
    encode(p->second, bl);
  }
}
template<class T, class U, class C>
inline void encode(const std::map<T,U,C>& m, bufferlist& bl)
{
  __u32 n = (__u32)(m.size());
  encode(n, bl);
  for (typename std::map<T,U,C>::const_iterator p = m.begin(); p != m.end(); ++p) {
    encode(p->first, bl);
    encode(p->second, bl);
  }
}
template<class T, class U>
inline void encode(const std::map<T,U>& m, bufferlist& bl, uint64_t features)
{
  __u32 n = (__u32)(m.size());
  encode(n, bl);
  for (typename std::map<T,U>::const_iterator p = m.begin(); p != m.end(); ++p) {
    encode(p->first, bl, features);
    encode(p->second, bl, features);
  }
}
template<class T, class U>
inline void decode(std::map<T,U>& m, bufferlist::iterator& p)
{
  __u32 n;
  decode(n, p);
  m.clear();
  while (n--) {
    T k;
    decode(k, p);
    decode(m[k], p);
  }
}
template<class T, class U, class C>
inline void decode(std::map<T,U,C>& m, bufferlist::iterator& p)
{
  __u32 n;
  decode(n, p);
  m.clear();
  while (n--) {
    T k;
    decode(k, p);
    decode(m[k], p);
  }
}
template<class T, class U, class C>
inline void decode_noclear(std::map<T,U,C>& m, bufferlist::iterator& p)
{
  __u32 n;
  decode(n, p);
  while (n--) {
    T k;
    decode(k, p);
    decode(m[k], p);
  }
}
template<class T, class U>
inline void decode_noclear(std::map<T,U>& m, bufferlist::iterator& p)
{
  __u32 n;
  decode(n, p);
  while (n--) {
    T k;
    decode(k, p);
    decode(m[k], p);
  }
}
template<class T, class U>
inline void encode_nohead(const std::map<T,U>& m, bufferlist& bl)
{
  for (typename std::map<T,U>::const_iterator p = m.begin(); p != m.end(); ++p) {
    encode(p->first, bl);
    encode(p->second, bl);
  }
}
template<class T, class U>
inline void encode_nohead(const std::map<T,U>& m, bufferlist& bl, uint64_t features)
{
  for (typename std::map<T,U>::const_iterator p = m.begin(); p != m.end(); ++p) {
    encode(p->first, bl, features);
    encode(p->second, bl, features);
  }
}
template<class T, class U>
inline void decode_nohead(int n, std::map<T,U>& m, bufferlist::iterator& p)
{
  m.clear();
  while (n--) {
    T k;
    decode(k, p);
    decode(m[k], p);
  }
}

// multimap
template<class T, class U>
inline void encode(const std::multimap<T,U>& m, bufferlist& bl)
{
  __u32 n = (__u32)(m.size());
  encode(n, bl);
  for (typename std::multimap<T,U>::const_iterator p = m.begin(); p != m.end(); ++p) {
    encode(p->first, bl);
    encode(p->second, bl);
  }
}
template<class T, class U>
inline void decode(std::multimap<T,U>& m, bufferlist::iterator& p)
{
  __u32 n;
  decode(n, p);
  m.clear();
  while (n--) {
    typename std::pair<T,U> tu = std::pair<T,U>();
    decode(tu.first, p);
    typename std::multimap<T,U>::iterator it = m.insert(tu);
    decode(it->second, p);
  }
}

// ceph::unordered_map
template<class T, class U>
inline void encode(const unordered_map<T,U>& m, bufferlist& bl,
		   uint64_t features)
{
  __u32 n = (__u32)(m.size());
  encode(n, bl);
  for (typename unordered_map<T,U>::const_iterator p = m.begin(); p != m.end(); ++p) {
    encode(p->first, bl, features);
    encode(p->second, bl, features);
  }
}
template<class T, class U>
inline void encode(const unordered_map<T,U>& m, bufferlist& bl)
{
  __u32 n = (__u32)(m.size());
  encode(n, bl);
  for (typename unordered_map<T,U>::const_iterator p = m.begin(); p != m.end(); ++p) {
    encode(p->first, bl);
    encode(p->second, bl);
  }
}
template<class T, class U>
inline void decode(unordered_map<T,U>& m, bufferlist::iterator& p)
{
  __u32 n;
  decode(n, p);
  m.clear();
  while (n--) {
    T k;
    decode(k, p);
    decode(m[k], p);
  }
}

// ceph::unordered_set
template<class T>
inline void encode(const ceph::unordered_set<T>& m, bufferlist& bl)
{
  __u32 n = (__u32)(m.size());
  encode(n, bl);
  for (typename ceph::unordered_set<T>::const_iterator p = m.begin(); p != m.end(); ++p)
    encode(*p, bl);
}
template<class T>
inline void decode(ceph::unordered_set<T>& m, bufferlist::iterator& p)
{
  __u32 n;
  decode(n, p);
  m.clear();
  while (n--) {
    T k;
    decode(k, p);
    m.insert(k);
  }
}

// deque
template<class T>
inline void encode(const std::deque<T>& ls, bufferlist& bl, uint64_t features)
{
  __u32 n = ls.size();
  encode(n, bl);
  for (typename std::deque<T>::const_iterator p = ls.begin(); p != ls.end(); ++p)
    encode(*p, bl, features);
}
template<class T>
inline void encode(const std::deque<T>& ls, bufferlist& bl)
{
  __u32 n = ls.size();
  encode(n, bl);
  for (typename std::deque<T>::const_iterator p = ls.begin(); p != ls.end(); ++p)
    encode(*p, bl);
}
template<class T>
inline void decode(std::deque<T>& ls, bufferlist::iterator& p)
{
  __u32 n;
  decode(n, p);
  ls.clear();
  while (n--) {
    T v;
    decode(v, p);
    ls.push_back(v);
  }
}


/*
 * guards
 */

/**
 * start encoding block
 *
 * @param v current (code) version of the encoding
 * @param compat oldest code version that can decode it
 * @param bl bufferlist to encode to
 */
#define ENCODE_START(v, compat, bl)			     \
  __u8 struct_v = v, struct_compat = compat;		     \
  ::encode(struct_v, (bl));				     \
  ::encode(struct_compat, (bl));			     \
  buffer::list::iterator struct_compat_it = (bl).end();	     \
  struct_compat_it.advance(-1);				     \
  ceph_le32 struct_len;				             \
  struct_len = 0;                                            \
  ::encode(struct_len, (bl));				     \
  buffer::list::iterator struct_len_it = (bl).end();	     \
  struct_len_it.advance(-4);				     \
  do {

/**
 * finish encoding block
 *
 * @param bl bufferlist we were encoding to
 * @param new_struct_compat struct-compat value to use
 */
#define ENCODE_FINISH_NEW_COMPAT(bl, new_struct_compat)			\
  } while (false);							\
  struct_len = (bl).length() - struct_len_it.get_off() - sizeof(struct_len); \
  struct_len_it.copy_in(4, (char *)&struct_len);			\
  if (new_struct_compat) {						\
    struct_compat = new_struct_compat;					\
    struct_compat_it.copy_in(1, (char *)&struct_compat);		\
  }

#define ENCODE_FINISH(bl) ENCODE_FINISH_NEW_COMPAT(bl, 0)

#define DECODE_ERR_VERSION(func, v)			\
  (std::string(func) + " unknown encoding version > " #v)

#define DECODE_ERR_OLDVERSION(func, v)			\
  (std::string(func) + " no longer understand old encoding version < " #v)

#define DECODE_ERR_PAST(func) \
  (std::string(func) + " decode past end of struct encoding")

/**
 * check for very old encoding
 *
 * If the encoded data is older than oldestv, raise an exception.
 *
 * @param oldestv oldest version of the code we can successfully decode.
 */
#define DECODE_OLDEST(oldestv)						\
  if (struct_v < oldestv)						\
    throw buffer::malformed_input(DECODE_ERR_OLDVERSION(__PRETTY_FUNCTION__, v)); 

/**
 * start a decoding block
 *
 * @param v current version of the encoding that the code supports/encodes
 * @param bl bufferlist::iterator for the encoded data
 */
#define DECODE_START(v, bl)						\
  __u8 struct_v, struct_compat;						\
  ::decode(struct_v, bl);						\
  ::decode(struct_compat, bl);						\
  if (v < struct_compat)						\
    throw buffer::malformed_input(DECODE_ERR_VERSION(__PRETTY_FUNCTION__, v)); \
  __u32 struct_len;							\
  ::decode(struct_len, bl);						\
  if (struct_len > bl.get_remaining())					\
    throw buffer::malformed_input(DECODE_ERR_PAST(__PRETTY_FUNCTION__)); \
  unsigned struct_end = bl.get_off() + struct_len;			\
  do {

#define __DECODE_START_LEGACY_COMPAT_LEN(v, compatv, lenv, skip_v, bl)	\
  __u8 struct_v;							\
  ::decode(struct_v, bl);						\
  if (struct_v >= compatv) {						\
    __u8 struct_compat;							\
    ::decode(struct_compat, bl);					\
    if (v < struct_compat)						\
      throw buffer::malformed_input(DECODE_ERR_VERSION(__PRETTY_FUNCTION__, v)); \
  } else if (skip_v) {							\
    if ((int)bl.get_remaining() < skip_v)				\
      throw buffer::malformed_input(DECODE_ERR_PAST(__PRETTY_FUNCTION__)); \
    bl.advance(skip_v);							\
  }									\
  unsigned struct_end = 0;						\
  if (struct_v >= lenv) {						\
    __u32 struct_len;							\
    ::decode(struct_len, bl);						\
    if (struct_len > bl.get_remaining())				\
      throw buffer::malformed_input(DECODE_ERR_PAST(__PRETTY_FUNCTION__)); \
    struct_end = bl.get_off() + struct_len;				\
  }									\
  do {

/**
 * start a decoding block with legacy support for older encoding schemes
 *
 * The old encoding schemes has a __u8 struct_v only, or lacked either
 * the compat version or length.  Skip those fields conditionally.
 *
 * Most of the time, v, compatv, and lenv will all match the version
 * where the structure was switched over to the new macros.
 *
 * @param v current version of the encoding that the code supports/encodes
 * @param compatv oldest version that includes a __u8 compat version field
 * @param lenv oldest version that includes a __u32 length wrapper
 * @param bl bufferlist::iterator containing the encoded data
 */
#define DECODE_START_LEGACY_COMPAT_LEN(v, compatv, lenv, bl)		\
  __DECODE_START_LEGACY_COMPAT_LEN(v, compatv, lenv, 0, bl)

/**
 * start a decoding block with legacy support for older encoding schemes
 *
 * This version of the macro assumes the legacy encoding had a 32 bit
 * version
 *
 * The old encoding schemes has a __u8 struct_v only, or lacked either
 * the compat version or length.  Skip those fields conditionally.
 *
 * Most of the time, v, compatv, and lenv will all match the version
 * where the structure was switched over to the new macros.
 *
 * @param v current version of the encoding that the code supports/encodes
 * @param compatv oldest version that includes a __u8 compat version field
 * @param lenv oldest version that includes a __u32 length wrapper
 * @param bl bufferlist::iterator containing the encoded data
 */
#define DECODE_START_LEGACY_COMPAT_LEN_32(v, compatv, lenv, bl)		\
  __DECODE_START_LEGACY_COMPAT_LEN(v, compatv, lenv, 3, bl)

#define DECODE_START_LEGACY_COMPAT_LEN_16(v, compatv, lenv, bl)		\
  __DECODE_START_LEGACY_COMPAT_LEN(v, compatv, lenv, 1, bl)

/**
 * finish decode block
 *
 * @param bl bufferlist::iterator we were decoding from
 */
#define DECODE_FINISH(bl)						\
  } while (false);							\
  if (struct_end) {							\
    if (bl.get_off() > struct_end)					\
      throw buffer::malformed_input(DECODE_ERR_PAST(__PRETTY_FUNCTION__)); \
    if (bl.get_off() < struct_end)					\
      bl.advance(struct_end - bl.get_off());				\
  }

/*
 * Encoders/decoders to read from current offset in a file handle and
 * encode/decode the data according to argument types.
 */
inline ssize_t decode_file(int fd, std::string &str)
{
  bufferlist bl;
  __u32 len = 0;
  bl.read_fd(fd, sizeof(len));
  decode(len, bl);                                                                                                  
  bl.read_fd(fd, len);
  decode(str, bl);                                                                                                  
  return bl.length();
}

inline ssize_t decode_file(int fd, bufferptr &bp)
{
  bufferlist bl;
  __u32 len = 0;
  bl.read_fd(fd, sizeof(len));
  decode(len, bl);                                                                                                  
  bl.read_fd(fd, len);
  bufferlist::iterator bli = bl.begin();

  decode(bp, bli);                                                                                                  
  return bl.length();
}

#endif
