// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "crimson/osd/osd_operation.h"
#include "crimson/osd/pg.h"
#include "crimson/osd/scrub/scrub_machine.h"

namespace crimson::osd {

class PrimaryScrubProcess : public OperationT<PrimaryScrubProcess> {
public:
  explicit PrimaryScrubProcess(Ref<PG> pg);
  ~PrimaryScrubProcess();

  // imposed by `ShardService::start_operation<T>(...)`.
  seastar::future<> start();

private:
  static constexpr OperationTypeCode type =
    OperationTypeCode::primary_scrub_process;

  void print(std::ostream &) const final;
  void dump_detail(Formatter *f) const final;
};

} // namespace crimson::osd
