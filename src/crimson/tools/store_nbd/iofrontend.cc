// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "iofrontend.h"
#include "nbd_frontend.h"

IOFrontend::ref_t IOFrontend::get_io_frontend(BlockDriver &bdriver, config_t config)
{
  return std::make_unique<NBDFrontend>(bdriver, config);
}
