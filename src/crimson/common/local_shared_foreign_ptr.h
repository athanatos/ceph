// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

#include <seastar/core/smp.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>

namespace crimson {

/**
 * local_shared_foreign_ptr
 *
 * See seastar/include/seastar/core/shareded.hh:local_shared_foreign_ptr
 *
 * seastar::foreign_ptr wraps a smart ptr by proxying the copy() and destructor
 * operations back to the original core.  This works well except that copy()
 * requires a cross-core call.  We need a smart_ptr which allows cross-core
 * caching of (for example) OSDMaps, but we want to avoid the overhead inherent
 * in incrementing the source smart_ptr on every copy.  Thus,
 * local_shared_foreign_ptr maintains a core-local foreign_ptr back to the
 * original core instance with core-local ref counting.
 */
template <typename PtrType>
class local_shared_foreign_ptr {
  struct local_state_t : public boost::intrusive_ref_counter<
    local_state_t, boost::thread_unsafe_counter> {
    /// foreign ptr back to original instance
    seastar::foreign_ptr<PtrType> source_ptr;
    /// debugging -- core on which this local_shared_foreign_ptr originated
    unsigned cpu;

    local_state_t(
      seastar::foreign_ptr<PtrType> &&source_ptr,
      unsigned cpu) : source_ptr(std::move(source_ptr)), cpu(cpu) {}
  };

  using element_type = typename std::pointer_traits<PtrType>::element_type;
  using pointer = element_type*;

  boost::intrusive_ptr<local_state_t> parent;

  /// Wraps a pointer object and remembers the current core.
  local_shared_foreign_ptr(decltype(parent) &&parent)
    : parent(std::move(parent)) {}

  template <typename T>
  friend local_shared_foreign_ptr<T> make_local_shared_foreign(
    seastar::foreign_ptr<T> &&);

public:
  /// Constructs a null local_shared_foreign_ptr<>.
  local_shared_foreign_ptr() = default;

  /// Constructs a null local_shared_foreign_ptr<>.
  local_shared_foreign_ptr(std::nullptr_t) : local_shared_foreign_ptr() {}

  /// Moves a local_shared_foreign_ptr<> to another object.
  local_shared_foreign_ptr(local_shared_foreign_ptr&& other) = default;

  /// Copies a local_shared_foreign_ptr<>
  local_shared_foreign_ptr(const local_shared_foreign_ptr &other) = default;

  /// Releases reference to parent eventually releasing the contained foreign_ptr
  ~local_shared_foreign_ptr() = default;

  /// Creates a copy of this foreign ptr. Only works if the stored ptr is copyable.
  seastar::future<seastar::foreign_ptr<PtrType>> get_foreign() {
    return parent->source_ptr.copy();
  }

  /// Accesses the wrapped object.
  element_type& operator*() const noexcept { return *parent->source_ptr; }
  /// Accesses the wrapped object.
  element_type* operator->() const noexcept { return &*parent->source_ptr; }
  /// Access the raw pointer to the wrapped object.
  pointer get() const noexcept {
    return parent ? parent->source_ptr.get() : nullptr;
  }

  /// Return the owner-shard of the contained foreign_ptr.
  unsigned get_owner_shard() const noexcept {
    return parent->source_ptr.get_owner_shard();
  }

  /// Checks whether the wrapped pointer is non-null.
  operator bool() const noexcept { return static_cast<bool>(parent); }

  /// Move-assigns a \c local_shared_foreign_ptr<>.
  local_shared_foreign_ptr& operator=(local_shared_foreign_ptr&& other) noexcept {
    parent = std::move(other.parent);
    return *this;
  }
};

/// Wraps a smart_ptr T in a local_shared_foreign_ptr<>.
template <typename T>
local_shared_foreign_ptr<T> make_local_shared_foreign(
  seastar::foreign_ptr<T> &&ptr) {
  return local_shared_foreign_ptr<T>(
    new typename local_shared_foreign_ptr<T>::local_state_t{
      std::move(ptr),
      seastar::this_shard_id()
    });
}

/// Wraps a in a local_shared_foreign_ptr<>.
template <typename T>
local_shared_foreign_ptr<T> make_local_shared_foreign(T &&ptr) {
  return make_local_shared_foreign(
    seastar::make_foreign(std::forward<T>(ptr)));
}

}

namespace seastar {

template<typename T>
struct is_smart_ptr<crimson::local_shared_foreign_ptr<T>> : std::true_type {};

}
