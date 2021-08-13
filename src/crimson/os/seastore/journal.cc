// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "extent_placement_manager.h"
#include "journal.h"
#include "journal/segmented_journal.h"

namespace crimson::os::seastore::journal {

JournalRef make_segmented(
  SegmentManager &sm,
  ExtentReader &reader,
  SegmentProvider &provider)
{
  return std::make_unique<SegmentedJournal>(sm, reader, provider);
}

}
