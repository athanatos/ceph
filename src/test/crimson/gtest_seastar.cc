// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "gtest_seastar.h"

int main(int argc, char **argv)
{
  std::vector<const char*> args(argv, argv + argc);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
