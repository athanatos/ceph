// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef INTERVAL_MAP_H
#define INTERVAL_MAP_H

#include <map>
#include <vector>
#include <utility>
#include <boost/optional.hpp>

template <typename K, typename V, typename S>
/**
 * interval_map
 *
 * Maps intervals to values.  Erasing or inserting over an existing
 * range will use S::operator() to split any overlapping existing
 * values.
 *
 * Surprisingly, boost/icl/interval_map doesn't seem to be appropriate
 * for this use case.  The aggregation concept seems to assume
 * commutativity, which doesn't work if we want more recent insertions
 * to overwrite previous ones.
 */
class interval_map {
  S s;
  using map = std::map<K, std::pair<K, V> >;
  using mapiter = typename std::map<K, std::pair<K, V> >::iterator;
  using cmapiter = typename std::map<K, std::pair<K, V> >::const_iterator;
  map m;
  std::pair<mapiter, mapiter> get_range(K off, K len) {
    // fst is first iterator with end after off (may be end)
    auto fst = m.upper_bound(off);
    if (fst != m.begin())
      --fst;
    if (fst != m.end() && off >= (fst->first + fst->second.first))
      ++fst;

    // lst is first iterator with start after off + len (may be end)
    auto lst = m.lower_bound(off + len);
    return std::make_pair(fst, lst);
  }
  std::pair<cmapiter, cmapiter> get_range(K off, K len) const {
    // fst is first iterator with end after off (may be end)
    auto fst = m.upper_bound(off);
    if (fst != m.begin())
      --fst;
    if (fst != m.end() && off >= (fst->first + fst->second.first))
      ++fst;

    // lst is first iterator with start after off + len (may be end)
    auto lst = m.lower_bound(off + len);
    return std::make_pair(fst, lst);
  }
public:
  void clear() {
    m.clear();
  }
  void erase(K off, K len) {
    if (len == 0)
      return;
    auto range = get_range(off, len);
    std::vector<
      std::pair<
	K,
	std::pair<K, V>
	>> to_insert;
    for (auto i = range.first; i != range.second; ++i) {
      if (i->first < off) {
	to_insert.emplace_back(
	  std::make_pair(
	    i->first,
	    std::make_pair(
	      off - i->first,
	      s(0, off - i->first, i->second.second))));
      }
      if ((off + len) < (i->first + i->second.first)) {
	K nlen = (i->first + i->second.first) - (off + len);
	to_insert.emplace_back(
	  std::make_pair(
	    off + len,
	    std::make_pair(
	      nlen,
	      s(i->second.first - nlen, nlen, i->second.second))));
      }
    }
    m.erase(range.first, range.second);
    m.insert(to_insert.begin(), to_insert.end());
  }
  void insert(K off, K len, V &&v) {
    erase(off, len);
    m.insert(make_pair(off, std::make_pair(len, v)));
  }
  void insert(K off, K len, const V &v) {
    erase(off, len);
    m.insert(make_pair(off, std::make_pair(len, v)));
  }
  bool empty() const {
    return m.empty();
  }
  class const_iterator {
    cmapiter it;
    const_iterator(cmapiter &&it) : it(std::move(it)) {}
    const_iterator(const cmapiter &it) : it(it) {}

    friend class interval_map;
  public:
    const_iterator(const const_iterator &) = default;
    const_iterator &operator=(const const_iterator &) = default;

    const_iterator &operator++() {
      ++it;
      return *this;
    }
    const_iterator operator++(int) {
      return const_iterator(it++);
    }
    const_iterator &operator--() {
      --it;
      return *this;
    }
    const_iterator operator--(int) {
      return const_iterator(it--);
    }
    bool operator==(const const_iterator &rhs) const {
      return it == rhs.it;
    }
    bool operator!=(const const_iterator &rhs) const {
      return it != rhs.it;
    }
    K get_off() const {
      return it->first;
    }
    K get_len() const {
      return it->second.first;
    }
    const V &get_val() const {
      return it->second.second;
    }
    const_iterator &operator*() {
      return *this;
    }
  };
  const_iterator begin() const {
    return const_iterator(m.begin());
  }
  const_iterator end() const {
    return const_iterator(m.end());
  }
  std::pair<const_iterator, const_iterator> get_containing_range(
    K off,
    K len) const {
    auto rng = get_range(off, len);
    return std::make_pair(const_iterator(rng.first), const_iterator(rng.second));
  }
  unsigned ext_count() const {
    return m.size();
  }
};

#endif
