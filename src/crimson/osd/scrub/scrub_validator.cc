// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/osd/scrub/scrub_validator.h"

namespace crimson::osd::scrub {

using object_set_t = std::set<hobject_t>;
object_set_t get_object_set(const scrub_map_set_t &in)
{
  object_set_t ret;
  for (const auto& [from, map] : in) {
    std::transform(map.objects.begin(), map.objects.end(),
                   std::inserter(ret, ret.end()),
                   [](const auto& i) { return i.first; });
  }
  return ret;
}

std::pair<pg_shard_t, const ScrubMap::object &>
select_auth_object(const hobject_t &hoid, const scrub_map_set_t &maps)
{
  // TODO: very wrong
  return std::make_pair(
    maps.begin()->first,
    std::cref(maps.begin()->second.objects.find(hoid)->second));
}

void validate_object(const hobject_t &hoid, const scrub_map_set_t &maps)
{

  auto auth_map_iter = select_auth_object(hoid, maps);
#if 0
  // clear per-object data:
  this_chunk->cur_inconsistent.clear();
  this_chunk->cur_missing.clear();
  this_chunk->fix_digest = false;

  stringstream candidates_errors;
  auto auth_res = select_auth_object(ho, candidates_errors);
  if (candidates_errors.str().size()) {
    // a collection of shard-specific errors detected while
    // finding the best shard to serve as authoritative
    clog.error() << candidates_errors.str();
  }

  inconsistent_obj_wrapper object_error{ho};
  if (!auth_res.is_auth_available) {
    // no auth selected
    object_error.set_version(0);
    object_error.set_auth_missing(ho,
                                  this_chunk->received_maps,
                                  auth_res.shard_map,
                                  this_chunk->m_error_counts.shallow_errors,
                                  this_chunk->m_error_counts.deep_errors,
                                  m_pg_whoami);

    if (object_error.has_deep_errors()) {
      this_chunk->m_error_counts.deep_errors++;
    } else if (object_error.has_shallow_errors()) {
      this_chunk->m_error_counts.shallow_errors++;
    }

    this_chunk->m_inconsistent_objs.push_back(std::move(object_error));
    return fmt::format("{} soid {} : failed to pick suitable object info\n",
                       m_scrubber.get_pgid().pgid,
                       ho);
  }

  stringstream errstream;
  auto& auth = auth_res.auth;

  // an auth source was selected

  object_error.set_version(auth_res.auth_oi.user_version);
  ScrubMap::object& auth_object = auth->second.objects[ho];
  ceph_assert(!this_chunk->fix_digest);

  auto [auths, objerrs] =
    match_in_shards(ho, auth_res, object_error, errstream);

  auto opt_ers =
    for_empty_auth_list(std::move(auths),
                        std::move(objerrs),
                        auth,
                        ho,
                        errstream);

  if (opt_ers.has_value()) {

    // At this point auth_list is populated, so we add the object error
    // shards as inconsistent.
    inconsistents(ho,
                  auth_object,
                  auth_res.auth_oi,
                  std::move(*opt_ers),
                  errstream);
  } else {

    // both the auth & errs containers are empty
    errstream << m_pg_id << " soid " << ho << " : empty auth list\n";
  }

  if (object_error.has_deep_errors()) {
    this_chunk->m_error_counts.deep_errors++;
  } else if (object_error.has_shallow_errors()) {
    this_chunk->m_error_counts.shallow_errors++;
  }

  if (object_error.errors || object_error.union_shards.errors) {
    this_chunk->m_inconsistent_objs.push_back(std::move(object_error));
  }

  if (errstream.str().empty()) {
    return std::nullopt;
  } else {
    return errstream.str();
  }
#endif
}

chunk_result_t validate_chunk(const chunk_info_t &in)
{
  auto all_objects = get_object_set(in.maps);

  

  return chunk_result_t{};
}

}
