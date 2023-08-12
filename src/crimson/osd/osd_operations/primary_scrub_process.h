// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "crimson/osd/osd_operation.h"
#include "crimson/osd/scrub/scrub_machine.h"

namespace crimson::osd {
class PG;

class PrimaryScrubProcess : public OperationT<PrimaryScrubProcess> {
public:
  static constexpr OperationTypeCode type =
    OperationTypeCode::primary_scrub_process;

  explicit PrimaryScrubProcess(Ref<PG> pg);

  ~PrimaryScrubProcess();

  seastar::future<> start();

private:
  Ref<PG> pg;

  void print(std::ostream &) const final;
  void dump_detail(Formatter *f) const final;
};

}
