// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#pragma once

#include <algorithm>
#include <string>
#include <vector>

#include "include/byteorder.h"
#include "include/encoding.h"
#include "include/denc.h"

namespace crimson::os::seastore::collection_manager {
struct coll_map_t {
  std::string key;
  uint32_t val;

  coll_map_t() = default;
  coll_map_t(const std::string &key, uint32_t val)
  : key(key),
    val(val) {}

  coll_map_t(const std::string &key)
  : key(key) {}

  bool operator==(const coll_map_t &s) {
    return  key == s.key;
  }

  DENC(coll_map_t, v, p) {
    DENC_START(1, 1, p);
    denc(v.key, p);
    denc(v.val, p);
    DENC_FINISH(p);
  }
};
}
WRITE_CLASS_DENC(crimson::os::seastore::collection_manager::coll_map_t)

namespace crimson::os::seastore::collection_manager {
class FlatNodeLayout;
struct delta_t {
  enum class op_t : uint_fast8_t {
    INSERT,
    UPDATE,
    REMOVE,
  } op;
  std::string key;
  uint32_t val;

  DENC(delta_t, v, p) {
    DENC_START(1, 1, p);
    denc(v.op, p);
    denc(v.key, p);
    denc(v.val, p);
    DENC_FINISH(p);
  }

  void replay(FlatNodeLayout &l);

  bool operator==(const delta_t &rhs) const {
    return op == rhs.op && key == rhs.key && val == rhs.val;
  }
};
}
WRITE_CLASS_DENC(crimson::os::seastore::collection_manager::delta_t)

namespace crimson::os::seastore::collection_manager {
class delta_buffer_t {
  std::vector<delta_t> buffer;
public:
  bool empty() const {
    return buffer.empty();
  }

  void insert(const std::string &key, uint32_t val) {
    buffer.push_back(delta_t{delta_t::op_t::INSERT, key, val});
  }
  void update(const std::string &key, uint32_t val) {
    buffer.push_back(delta_t{delta_t::op_t::UPDATE, key, val});
  }
  void remove(const std::string &key) {
    buffer.push_back(delta_t{delta_t::op_t::REMOVE, key, uint32_t()});
  }
  void replay(FlatNodeLayout &node) {
    for (auto &i: buffer) {
      i.replay(node);
    }
  }

  DENC(delta_buffer_t, v, p) {
    DENC_START(1, 1, p);
    denc(v.buffer, p);
    DENC_FINISH(p);
  }
};
}
WRITE_CLASS_DENC(crimson::os::seastore::collection_manager::delta_buffer_t)

namespace crimson::os::seastore::collection_manager {
class FlatNodeLayout {
  friend class delta_t;
public:
  std::vector<coll_map_t> coll_kv;

  bool journal_insert(const std::string &key, uint32_t val, delta_buffer_t *recorder) {
    if (recorder) {
      recorder->insert(key, val);
    }
    return insert(key, val);
  }

 bool journal_update(const std::string &key, uint32_t val, delta_buffer_t *recorder) {
    if (recorder) {
      recorder->update(key, val);
    }

    return update(key, val);
  }

  bool journal_remove(const std::string &key, delta_buffer_t *recorder) {
    if (recorder) {
      recorder->remove(key);
    }

    return remove(key);
  }

  FlatNodeLayout() = default;
  virtual ~FlatNodeLayout() = default;

  bool is_overflow(std::string &key, uint32_t val, size_t node_size) {
    coll_kv.push_back({key, val});
    auto final_size = encoded_sizeof(coll_kv);
    coll_kv.pop_back();
    return final_size > node_size;
  }

  size_t get_size() const {
    return coll_kv.size();
  }

private:
  bool insert(const std::string &key, uint32_t val) {
    coll_kv.emplace_back(coll_map_t(key, val));
    return true;
  }

  bool remove(const std::string &key) {
    coll_map_t s{key};
    auto it = std::find(coll_kv.begin(), coll_kv.end(), s);
    if (it != coll_kv.end()) {
      coll_kv.erase(it);
      return true;
    }
    else return false;
  }

  bool update(const std::string &key, uint32_t val) {
    coll_map_t s{key};
    auto it = std::find(coll_kv.begin(), coll_kv.end(), s);
    if (it != coll_kv.end()){
      it->val = val;
      return true;
    }
    else return false;
  }
};

inline void delta_t::replay(FlatNodeLayout &l) {
  switch (op) {
  case op_t::INSERT: {
    l.insert(key, val);
    break;
  }
  case op_t::UPDATE: {
    l.update(key, val);
    break;
  }
  case op_t::REMOVE: {
    l.remove(key);
    break;
  }
  default:
    assert ( 0 == "Impossible");
  }
}
}

