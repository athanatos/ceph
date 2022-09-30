// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "crimson/osd/osd.h"
#include "crimson/osd/osdmap_gate.h"
#include "crimson/osd/osd_operations/background_recovery.h"
#include "crimson/osd/osd_operations/client_request.h"
#include "crimson/osd/osd_operations/peering_event.h"
#include "crimson/osd/osd_operations/pg_advance_map.h"
#include "crimson/osd/osd_operations/recovery_subrequest.h"
#include "crimson/osd/osd_operations/replicated_request.h"
#include "crimson/osd/pg_activation_blocker.h"
#include "crimson/osd/pg_map.h"

namespace crimson::osd {

// Placeholder for eventual implementation
struct LttngBackend {
  // Default noop
  template <typename... Args>
  void handle(Args&&...) {}
};

struct HistoricBackend {
  static const ClientRequest& to_client_request(const Operation& op) {
#ifdef NDEBUG
    return static_cast<const ClientRequest&>(op);
#else
    return dynamic_cast<const ClientRequest&>(op);
#endif
  }

  void handle(ClientRequest::CompletionEvent&, const Operation& op) {
    if (crimson::common::local_conf()->osd_op_history_size) {
      to_client_request(op).put_historic();
    }
  }

  // Default noop
  template <typename... Args>
  void handle(Args&&...) {}
};

} // namespace crimson::osd

namespace crimson {

template <>
struct EventBackendRegistry<osd::ClientRequest> {
  static std::tuple<osd::LttngBackend, osd::HistoricBackend> get_backends() {
    return { {}, {} };
  }
};

template <>
struct EventBackendRegistry<osd::RemotePeeringEvent> {
  static std::tuple<> get_backends() {
    return {/* no extenral backends */};
  }
};

template <>
struct EventBackendRegistry<osd::LocalPeeringEvent> {
  static std::tuple<> get_backends() {
    return {/* no extenral backends */};
  }
};

template <>
struct EventBackendRegistry<osd::RepRequest> {
  static std::tuple<> get_backends() {
    return {/* no extenral backends */};
  }
};


template <>
struct EventBackendRegistry<osd::LogMissingRequest> {
  static std::tuple<> get_backends() {
    return {/* no extenral backends */};
  }
};

template <>
struct EventBackendRegistry<osd::LogMissingRequestReply> {
  static std::tuple<> get_backends() {
    return {/* no extenral backends */};
  }
};

template <>
struct EventBackendRegistry<osd::RecoverySubRequest> {
  static std::tuple<> get_backends() {
    return {/* no extenral backends */};
  }
};

template <>
struct EventBackendRegistry<osd::BackfillRecovery> {
  static std::tuple<> get_backends() {
    return {};
  }
};

template <>
struct EventBackendRegistry<osd::PGAdvanceMap> {
  static std::tuple<> get_backends() {
    return {};
  }
};

} // namespace crimson
