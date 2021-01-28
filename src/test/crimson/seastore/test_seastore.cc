// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <string>
#include <iostream>
#include <sstream>

#include "test/crimson/gtest_seastar.h"

#include "test/crimson/seastore/transaction_manager_test_state.h"

#include "crimson/os/futurized_collection.h"
#include "crimson/os/seastore/seastore.h"

using namespace crimson;
using namespace crimson::os;
using namespace crimson::os::seastore;
using CTransaction = ceph::os::Transaction;
using namespace std;

namespace {
  [[maybe_unused]] seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_test);
  }
}


struct seastore_test_t :
  public seastar_test_suite_t,
  SeaStoreTestState {

  coll_t coll_name{spg_t{pg_t{0, 0}}};
  CollectionRef coll;

  seastore_test_t() {}

  seastar::future<> set_up_fut() final {
    return tm_setup(
    ).then([this] {
      coll = seastore->create_new_collection(coll_name).get0();
      CTransaction t;
      t.create_collection(coll_name, 16);
      do_transaction(std::move(t));
    });
  }

  seastar::future<> tear_down_fut() final {
    return tm_teardown();
  }

  void do_transaction(CTransaction &&t) {
    return seastore->do_transaction(
      coll,
      std::move(t)).get0();
  }
};

ghobject_t make_oid(int i) {
  stringstream ss;
  ss << "object_" << i;
  return ghobject_t(
    hobject_t(
      sobject_t(ss.str(), CEPH_NOSNAP)));
}

TEST_F(seastore_test_t, basic)
{
  run_async([this] {
    CTransaction t;
    do_transaction(std::move(t));
  });
}
