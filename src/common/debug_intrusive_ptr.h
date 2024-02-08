// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <set>

#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

#include "include/ceph_assert.h"

namespace ceph {

template <typename T>
class debug_intrusive_ptr;

template <typename T>
class debug_intrusive_ref_counter :
  public boost::intrusive_ref_counter<T, boost::thread_unsafe_counter> {

  std::set<const debug_intrusive_ptr<T>*> backpointers;
  
  template <typename U>
  friend void check_set_backpointer(
    debug_intrusive_ref_counter<U> &, debug_intrusive_ptr<U> *);

  template <typename U>
  friend void check_clear_backpointer(
    debug_intrusive_ref_counter<U> &, debug_intrusive_ptr<U> *);

  template <typename U>
  friend bool _check_backpointer(
    const debug_intrusive_ref_counter<U> &, const debug_intrusive_ptr<U> *);
  
};

template <typename T>
bool _check_backpointer(
  const debug_intrusive_ref_counter<T> &o, const debug_intrusive_ptr<T> *backpointer) {
  return o.backpointers.contains(backpointer);
}

template <typename T>
void check_set_backpointer(
  debug_intrusive_ref_counter<T> &o, debug_intrusive_ptr<T> *backpointer) {
  ceph_assert_always(!o.backpointers.contains(backpointer));
  o.backpointers.insert(backpointer);
}

template <typename T>
void check_clear_backpointer(
  debug_intrusive_ref_counter<T> &o, debug_intrusive_ptr<T> *backpointer) {
  ceph_assert_always(o.backpointers.contains(backpointer));
  o.backpointers.erase(backpointer);
}

template <typename T>
class debug_intrusive_ptr : public boost::intrusive_ptr<T> {
  void _check_set_backpointer() {
    if (boost::intrusive_ptr<T>::get()) {
      check_set_backpointer(**this, this);
    }
  }
  void _check_clear_backpointer() {
    if (boost::intrusive_ptr<T>::get()) {
      check_clear_backpointer(**this, this);
    }
  }
  void reset() {
    _check_clear_backpointer();
    boost::intrusive_ptr<T>::operator=(nullptr);
  }
public: 
  debug_intrusive_ptr() = default;
  debug_intrusive_ptr(T *o)
    : boost::intrusive_ptr<T>(o) {
    _check_set_backpointer();
  }
  debug_intrusive_ptr(boost::intrusive_ptr<T> o)
    : boost::intrusive_ptr<T>(o) {
    _check_set_backpointer();
  }
  debug_intrusive_ptr(const debug_intrusive_ptr &o)
    : debug_intrusive_ptr(
      static_cast<const boost::intrusive_ptr<T>&>(o)
    ) {}
  debug_intrusive_ptr(debug_intrusive_ptr &&o)
    : debug_intrusive_ptr(
      static_cast<const boost::intrusive_ptr<T>&>(o)
    ) {}
  debug_intrusive_ptr &operator=(const debug_intrusive_ptr &o) {
    _check_clear_backpointer();
    boost::intrusive_ptr<T>::operator=(o);
    _check_set_backpointer();
    return *this;
  }
  debug_intrusive_ptr &operator=(debug_intrusive_ptr &&o) {
    _check_clear_backpointer();
    boost::intrusive_ptr<T>::operator=(o);
    _check_set_backpointer();
    return *this;
  }
  debug_intrusive_ptr &operator=(const boost::intrusive_ptr<T> &o) {
    _check_clear_backpointer();
    boost::intrusive_ptr<T>::operator=(o);
    _check_set_backpointer();
    return *this;
  }
  debug_intrusive_ptr &operator=(boost::intrusive_ptr<T> &&o) {
    _check_clear_backpointer();
    boost::intrusive_ptr<T>::operator=(o);
    _check_set_backpointer();
    return *this;
  }
  void check_backpointer() const {
    if (boost::intrusive_ptr<T>::get()) {
      ceph_assert_always(_check_backpointer(**this, this));
    } else {
      ceph_assert_always(!_check_backpointer(**this, this));
    }
  }
  ~debug_intrusive_ptr() {
    _check_clear_backpointer();
  }
};
  
}
