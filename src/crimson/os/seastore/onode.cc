// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "onode.h"
#include "include/encoding.h"

namespace crimson::os::seastore {

std::ostream& operator<<(std::ostream &out, const Onode &rhs)
{
  return out << "Onode("
	     << "size=" << rhs.get_object_size()
	     << ")";
}

}

