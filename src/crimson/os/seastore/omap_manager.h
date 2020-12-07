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

struct list_keys_result_t {
  std::vector<std::string> keys;
  std::string next;
};

struct list_kvs_result_t {
  std::vector<std::pair<std::string, std::string>> kvs;
  std::string next;
};
constexpr size_t MAX_SIZE = std::numeric_limits<size_t>::max();
std::ostream &operator<<(std::ostream &out, const std::list<std::string> &rhs);
std::ostream &operator<<(std::ostream &out, const std::map<std::string, std::string> &rhs);

class OMapManager {
 /* all OMapManager API use reference to transfer input string parameters,
  * the upper caller should guarantee the referenced string values alive (not freed)
  * until these functions future resolved.
  */
public:
  /* allocate omap tree root node
   *
   * input: Transaction &t, current transaction
   * return: return the omap_root_t structure.
   */
  using initialize_omap_ertr = TransactionManager::alloc_extent_ertr;
  using initialize_omap_ret = initialize_omap_ertr::future<omap_root_t>;
  virtual initialize_omap_ret initialize_omap(Transaction &t) = 0;

  /*get value(string) by key(string)
   *
   * input: omap_root_t omap_root,  omap btree root information
   * input: Transaction &t,  current transaction
   * input: string &key, omap string key
   * return: string key->string value mapping pair.
   */
  using omap_get_value_ertr = TransactionManager::read_extent_ertr;
  using omap_get_value_ret = omap_get_value_ertr::future<std::pair<std::string, std::string>>;
  virtual omap_get_value_ret omap_get_value(omap_root_t &omap_root, Transaction &t,
		                            const std::string &key) = 0;

  /* set key value mapping in omap
   *
   * input: omap_root_t &omap_root,  omap btree root information
   * input: Transaction &t,  current transaction
   * input: string &key, omap string key
   * input: string &value, mapped value corresponding key
   * return: mutation_result_t, status should be success.
   */
  using omap_set_key_ertr = TransactionManager::read_extent_ertr;
  using omap_set_key_ret = omap_set_key_ertr::future<bool>;
  virtual omap_set_key_ret omap_set_key(omap_root_t &omap_root, Transaction &t,
		                        const std::string &key, const std::string &value) = 0;

  /* remove key value mapping in omap tree
   *
   * input: omap_root_t &omap_root,  omap btree root information
   * input: Transaction &t,  current transaction
   * input: string &key, omap string key
   * return: remove success return true, else return false.
   */
  using omap_rm_key_ertr = TransactionManager::read_extent_ertr;
  using omap_rm_key_ret = omap_rm_key_ertr::future<bool>;
  virtual omap_rm_key_ret omap_rm_key(omap_root_t &omap_root, Transaction &t,
		                                    const std::string &key) = 0;

  /* get all keys or partial keys in omap tree
   *
   * input: omap_root_t &omap_root,  omap btree root information
   * input: Transaction &t,  current transaction
   * input: string &start, the list keys range begin from start,
   *        if start is "", list from the first omap key
   * input: max_result_size, the number of list keys,
   *        it it is not set, list all keys after string start
   * return: list_keys_result_t, listed keys and next key
   */
  using omap_list_keys_ertr = TransactionManager::read_extent_ertr;
  using omap_list_keys_ret = omap_list_keys_ertr::future<list_keys_result_t>;
  virtual omap_list_keys_ret omap_list_keys(omap_root_t &omap_root, Transaction &t,
                             std::string &start,
                             size_t max_result_size = MAX_SIZE) = 0;

  /* Get all or partial key-> value mapping in omap tree
   *
   * input: omap_root_t &omap_root,  omap btree root information
   * input: Transaction &t,  current transaction
   * input: string &start, the list keys range begin from start,
   *        if start is "" , list from the first omap key
   * input: max_result_size, the number of list keys,
   *        it it is not set, list all keys after string start.
   * return: list_kvs_result_t, listed key->value mapping and next key.
   */
  using omap_list_ertr = TransactionManager::read_extent_ertr;
  using omap_list_ret = omap_list_ertr::future<list_kvs_result_t>;
  virtual omap_list_ret omap_list(omap_root_t &omap_root, Transaction &t,
                        std::string &start,
                        size_t max_result_size = MAX_SIZE) = 0;

  /* clear all omap tree key->value mapping
   *
   * input: omap_root_t &omap_root,  omap btree root information
   * input: Transaction &t,  current transaction
   */
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
