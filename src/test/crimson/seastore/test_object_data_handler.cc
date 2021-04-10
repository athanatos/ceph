// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/crimson/gtest_seastar.h"
#include "test/crimson/seastore/transaction_manager_test_state.h"

#include "crimson/os/seastore/onode.h"
#include "crimson/os/seastore/object_data_handler.h"

using namespace crimson;
using namespace crimson::os;
using namespace crimson::os::seastore;

namespace {
  [[maybe_unused]] seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_test);
  }
}

class TestOnode : public Onode {
  onode_layout_t layout;
  bool dirty = false;

public:
  const onode_layout_t &get_layout() const final {
    return layout;
  }
  onode_layout_t &get_mutable_layout(Transaction &t) final {
    dirty = true;
    return layout;
  }
  bool is_dirty() const { return dirty; }
  ~TestOnode() final = default;
};

struct object_data_handler_test_t:
  public seastar_test_suite_t,
  TMTestState {
  OnodeRef onode;

  bufferptr known_contents;
  extent_len_t size = 0;

  object_data_handler_test_t() {}

  void write(Transaction &t, objaddr_t offset, extent_len_t len, char fill) {
    ceph_assert(offset + len <= known_contents.length());
    size = std::max<extent_len_t>(size, offset + len);
    memset(
      known_contents.c_str() + offset,
      fill,
      len);
    bufferlist bl;
    bl.append(
      bufferptr(
	known_contents,
	offset,
	len));
    return ObjectDataHandler().write(
      ObjectDataHandler::context_t{
	*tm,
	t,
	*onode,
      },
      offset,
      bl).unsafe_get0();
  }
  void write(objaddr_t offset, extent_len_t len, char fill) {
    auto t = tm->create_transaction();
    write(*t, offset, len, fill);
    return tm->submit_transaction(std::move(t)).unsafe_get0();
  }

  void read(Transaction &t, objaddr_t offset, extent_len_t len) {
    ceph_assert(offset + len <= size);
    bufferlist bl = ObjectDataHandler().read(
      ObjectDataHandler::context_t{
	*tm,
	t,
	*onode
      },
      offset,
      len).unsafe_get0();
    bufferlist known;
    known.append(
      bufferptr(
	known_contents,
	offset,
	len));
    EXPECT_EQ(bl.length(), known.length());
    EXPECT_EQ(bl, known);
  }
  void read(objaddr_t offset, extent_len_t len) {
    auto t = tm->create_transaction();
    read(*t, offset, len);
  }

  seastar::future<> set_up_fut() final {
    onode = new TestOnode{};
    known_contents = buffer::create(4<<20 /* 4MB */);
    size = 0;
    return tm_setup();
  }

  seastar::future<> tear_down_fut() final {
    onode.reset();
    size = 0;
    return tm_teardown();
  }
};

TEST_F(object_data_handler_test_t, single_write_read)
{
  run_async([this] {
    write(1<<20, 4<<10, 'c');
    read(1<<20, 4<<10);
  });
}

TEST_F(object_data_handler_test_t, single_write_unaligned_read_right)
{
  run_async([this] {
    write(1<<20, 8<<10, 'c');
    read(1<<20, (4<<10) + 511);
  });
}

TEST_F(object_data_handler_test_t, single_write_unaligned_read_left)
{
  run_async([this] {
    write(1<<20, 8<<10, 'c');
    read((1<<20) - 511, (4<<10) + 511);
  });
}

TEST_F(object_data_handler_test_t, single_write_unaligned_read_both)
{
  run_async([this] {
    write(1<<20, 8<<10, 'c');
    read((1<<20) - 511, 4<<10);
  });
}

TEST_F(object_data_handler_test_t, multi_write_aligned_read)
{
  run_async([this] {
    write((1<<20) - (4<<10), 4<<10, 'a');
    write(1<<20, 4<<10, 'b');
    write((1<<20) + (4<<10), 4<<10, 'c');

    read((1<<20) - (4<<10), 12<<10);
  });
}
