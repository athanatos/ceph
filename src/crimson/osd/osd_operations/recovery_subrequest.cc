#include <fmt/format.h>
#include <fmt/ostream.h>

#include "crimson/osd/osd_operations/recovery_subrequest.h"
#include "crimson/osd/osd_connection_priv.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_osd);
  }
}

namespace crimson::osd {

seastar::future<> RecoverySubRequest::with_pg(
  ShardServices &shard_services, Ref<PG> pgref)
{
  logger().debug("{}: {}", "RecoverySubRequest::with_pg_int", *this);

  IRef opref = this;
  return interruptor::with_interruption([this, opref, pgref] {
    return pgref->get_recovery_backend()->handle_recovery_op(m);
  }, [opref, pgref](std::exception_ptr) { return seastar::now(); }, pgref);
}

ConnectionPipeline &RecoverySubRequest::get_connection_pipeline()
{
  return get_osd_priv(conn.get()).peering_request_conn_pipeline;
}

}
