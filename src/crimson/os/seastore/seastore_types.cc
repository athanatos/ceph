// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/os/seastore/seastore_types.h"

namespace crimson::os::seastore {

std::ostream &operator<<(std::ostream &out, const paddr_t &rhs) {
  return out << "paddr_t<" << rhs.segment << ", " << rhs.offset << ">";
}

}
