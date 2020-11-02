// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <iostream>

#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

#include <seastar/core/future.hh>

#include "crimson/osd/exceptions.h"
#include "crimson/os/seastore/seastore_types.h"
#include "crimson/os/seastore/transaction_manager.h"

#define OMAP_BLOCK_SIZE 4096

namespace crimson::os::seastore {

enum class omap_root_state_t : uint8_t {
  INITIAL = 0,
  MUTATED = 1,
  NONE = 0xFF
};

struct omap_root_t {
  depth_t depth = 0;
  omap_root_state_t state;
  laddr_t omap_root_laddr;
  omap_root_t(depth_t dep, laddr_t laddr)
  : depth(dep),
    omap_root_laddr(laddr) { state = omap_root_state_t::INITIAL; }
};

std::ostream &operator<<(std::ostream &out, const std::list<std::string> &rhs);
std::ostream &operator<<(std::ostream &out, const std::map<std::string, std::string> &rhs);

class OMapManager {
public:
  using initialize_omap_ertr = TransactionManager::alloc_extent_ertr;
  using initialize_omap_ret = initialize_omap_ertr::future<omap_root_t>;
  virtual initialize_omap_ret initialize_omap(Transaction &t) = 0;

  /*get value by key
   */
  using omap_get_value_ertr = TransactionManager::read_extent_ertr;
  using omap_get_value_ret = omap_get_value_ertr::future<std::pair<std::string, std::string>>;
  virtual omap_get_value_ret omap_get_value(omap_root_t &omap_root, Transaction &t,
		                            const std::string &key) = 0;

  /* set value by key
   */
  using omap_set_key_ertr = TransactionManager::read_extent_ertr;
  using omap_set_key_ret = omap_set_key_ertr::future<std::pair<std::string, std::string>>;
  virtual omap_set_key_ret omap_set_key(omap_root_t &omap_root, Transaction &t,
		                        std::string &key, std::string &value) = 0;

  /* rm key value
   */
  using omap_rm_key_ertr = TransactionManager::read_extent_ertr;
  using omap_rm_key_ret = omap_rm_key_ertr::future<bool>;
  virtual omap_rm_key_ret omap_rm_key(omap_root_t &omap_root, Transaction &t,
		                                    const std::string &key) = 0;

  /* get all keys
   */
  using omap_list_keys_ertr = TransactionManager::read_extent_ertr;
  using omap_list_keys_ret = omap_list_keys_ertr::future<std::vector<std::string>>;
  virtual omap_list_keys_ret omap_list_keys(omap_root_t &omap_root, Transaction &t) = 0;

  /* Get all keys and values
   */
  using omap_list_ertr = TransactionManager::read_extent_ertr;
  using omap_list_ret = omap_list_ertr::future<std::vector<std::pair<std::string, std::string>>>;
  virtual omap_list_ret omap_list(omap_root_t &omap_root, Transaction &t) = 0;

  using omap_clear_ertr = TransactionManager::read_extent_ertr;
  using omap_clear_ret = omap_clear_ertr::future<>;
  virtual omap_clear_ret omap_clear(omap_root_t &omap_root, Transaction &t) = 0;

  virtual ~OMapManager() {}
};
using OMapManagerRef = std::unique_ptr<OMapManager>;

namespace omap_manager {

OMapManagerRef create_omap_manager (
  TransactionManager &trans_manager);
}

}
