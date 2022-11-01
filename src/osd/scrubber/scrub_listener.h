// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "osd/osd_types.h"

namespace Scrub {

/**
 * ScrubListener
 *
 * Interface by which PgScrubber propogates external events and
 * accesses external interfaces.
 */
class ScrubListener {
public:
  /// get spg_t for pg
  virtual spg_t sl_get_spgid() const = 0;

  /// returns true iff pg has reset peering since e.
  virtual bool sl_has_reset_since(epoch_t e) const = 0;

  /// return reference to current osdmap
  virtual const OSDMapRef &sl_get_osdmap() const = 0;

  /// get current osdmap epoch
  epoch_t sl_get_osdmap_epoch() const { return sl_get_osdmap()->get_epoch(); }

  /// get reference to PGPool
  virtual const PGPool &sl_get_pool() const = 0;

  /// get config reference
  virtual const ConfigProxy &sl_get_config() const = 0;

  virtual ~ScrubListener() = default;
};

}
