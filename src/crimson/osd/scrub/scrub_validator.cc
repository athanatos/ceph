// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/osd/scrub/scrub_validator.h"
// enable once ec is merged #include "osd/ECUtil.h"

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

struct shard_validator_t {
  bool present{true};
  std::optional<shard_info_wrapper> shard_info;

  std::optional<object_info_t> object_info;
  std::optional<SnapSet> snapset;
  std::optional<ECUtil::HashInfo> hinfo;

  static shard_validator_t absent() {
    return shard_validator_t{false};
  }

  static shard_validator_t present() {
    return shard_validator_t{
      true,
      shard_info_wrapper{}
    };
  }
};
shard_valdiator_t generate_shard_info(
  const chunk_validation_policy_t &params,
  const hobject_t &oid,
  const ScrubMap::object &obj)
{
  if (obj.negative) {
    ceph_assert(0 == "impossible since chunky scrub was introduced");
    return shard_validator_t::absent();
  }

  auto ret = shard_validator_t::present();

  ret.shard_info.size = obj.size;

  ret.shard_info.omap_digest_present = obj.omap_digest_present;
  ret.shard_info.omap_digest = obj.omap_digest;

  ret.shard_info.data_digest_present = obj.digest_present;
  ret.shard_info.data_digest = obj.digest;

  if (obj.ec_hash_mismatch) {
    ret.shard_info.set_ec_hash_mismatch();
  }

  if (obj.ec_size_mismatch) {
    ret.shard_info.set_ec_size_mismatch();
  }

  if (obj.read_error) {
    ret.shard_info.set_read_error();
  }

  if (obj.stat_error) {
    ret.shard_info.set_stat_error();
  }

  {
    auto xiter = obj.attrs.find(OI_ATTR);
    if (xiter == obj.attrs.end()) {
      ret.shard_info.set_info_missing();
    } else {
      bufferlist bl;
      bl.push_back(xiter->second);
      ret.object_info = object_info_t{};
      try {
	auto bliter = bl.cbegin();
	::decode(*(ret.object_info), bliter);
      } catch (...) {
	ret.shard_info.set_info_corrupted();
	ret.object_info = std::nullopt;
      }
    }
  }

  {
    auto xiter = obj.attrs.find(SS_ATTR);
    if (xiter == obj.attrs.end()) {
      ret.shard_info.set_snapset_missing();
    } else {
      bufferlist bl;
      bl.push_back(xiter->second);
      ret.snapset = SnapSet{}
      try {
	auto bliter = bl.cbegin();
	::decode(*(ret.snapset), bliter);
      } catch (...) {
	ret.shard_info.set_snapset_corrupted();
	ret.snapset = std::nullopt;
      }
    }
  }

#if 0 // enable once EC is implemented, need to link against ECUtils
  if (params.is_ec) {
    auto xiter = obj.attrs.find(ECUtil::get_hinfo_key());
    if (xiter == obj.attrs.end()) {
      ret.shard_info.set_hinfo_missing = true;
    } else {
      bufferlist bl;
      bl.push_back(xiter->second);
      ECUtil::HashInfo hinfo;
      try {
	auto bliter = bl.cbegin();
	::decode(hinfo, bliter);
      } catch (...) {
	ret.shard_info.set_hinfo_corrupted();
      }
    }
  }
#endif

  for (auto &[key, contents] : obj.attrs) {
    bufferlist bl;
    bl.push_back(contents);
    ret.shard_info.attrs.emplace(key, std::move(bl));
  }
  return ret;
}

std::pair<pg_shard_t, const ScrubMap::object &>
select_auth_object(
  const chunk_validation_policy_t &policy,
  const hobject_t &hoid,
  const scrub_map_set_t &maps)
{

  using obj_shard_map_t = std::map<
    pg_shard_t, std::optional<shard_info_wrapper>>;
  obj_shard_map_t shards;
  std::transform(
    maps.begin(),
    maps.end(),
    std::inserter(shards, shards.end()),
    [&hoid, &policy](const auto &item) -> obj_shard_map_t::value_type {
      const auto &[shard, scrub_map] = item;
      auto miter = scrub_map.objects.find(hoid);
      if (miter == scrub_map.objects.end()) {
	return obj_shard_map_t::value_type{
	  shard,
	  std::nullopt};
      } else {
	return obj_shard_map_t::value_type{
	  shard,
	  generate_shard_info(
	    policy, hoid, miter->second
	  )};
      }
    });
#if 0
  // Create a list of shards (with the Primary first, so that it will be
  // auth-copy, all other things being equal)

  /// \todo: consider sorting the candidate shards by the conditions for
  /// selecting best auth source below. Then - stopping on the first one
  /// that is auth eligible.
  /// This creates an issue with 'digest_match' that should be handled.
  std::list<pg_shard_t> shards;
  for (const auto& [srd, smap] : this_chunk->received_maps) {
    if (srd != m_pg_whoami) {
      shards.push_back(srd);
    }
  }
  shards.push_front(m_pg_whoami);

  auth_selection_t ret_auth;
  ret_auth.auth = this_chunk->received_maps.end();
  eversion_t auth_version;

  for (auto& l : shards) {

    auto shard_ret = possible_auth_shard(ho, l, ret_auth.shard_map);

    // digest_match will only be true if computed digests are the same
    if (auth_version != eversion_t() &&
        ret_auth.auth->second.objects[ho].digest_present &&
        shard_ret.digest.has_value() &&
        ret_auth.auth->second.objects[ho].digest != *shard_ret.digest) {

      ret_auth.digest_match = false;
      dout(10) << fmt::format(
                    "{}: digest_match = false, {} data_digest 0x{:x} != "
                    "data_digest 0x{:x}",
                    __func__,
                    ho,
                    ret_auth.auth->second.objects[ho].digest,
                    *shard_ret.digest)
               << dendl;
    }

    dout(20)
      << fmt::format("{}: {} shard {} got:{:D}", __func__, ho, l, shard_ret)
      << dendl;

    if (shard_ret.possible_auth == shard_as_auth_t::usable_t::not_usable) {

      // Don't use this particular shard due to previous errors
      // XXX: For now we can't pick one shard for repair and another's object
      // info or snapset

      ceph_assert(shard_ret.error_text.length());
      errstream << m_pg_id.pgid << " shard " << l << " soid " << ho << " : "
                << shard_ret.error_text << "\n";

    } else if (shard_ret.possible_auth ==
               shard_as_auth_t::usable_t::not_found) {

      // do not emit the returned error message to the log
      dout(15) << fmt::format("{}: {} not found on shard {}", __func__, ho, l)
               << dendl;
    } else {

      dout(30) << fmt::format("{}: consider using {} srv: {} oi soid: {}",
                              __func__,
                              l,
                              shard_ret.oi.version,
                              shard_ret.oi.soid)
               << dendl;

      // consider using this shard as authoritative. Is it more recent?

      if (auth_version == eversion_t() || shard_ret.oi.version > auth_version ||
          (shard_ret.oi.version == auth_version &&
           dcount(shard_ret.oi) > dcount(ret_auth.auth_oi))) {

        dout(20) << fmt::format("{}: using {} moved auth oi {:p} <-> {:p}",
                                __func__,
                                l,
                                (void*)&ret_auth.auth_oi,
                                (void*)&shard_ret.oi)
                 << dendl;

        ret_auth.auth = shard_ret.auth_iter;
        ret_auth.auth_shard = ret_auth.auth->first;
        ret_auth.auth_oi = shard_ret.oi;
        auth_version = shard_ret.oi.version;
        ret_auth.is_auth_available = true;
      }
    }
  }

  dout(10) << fmt::format("{}: selecting osd {} for obj {} with oi {}",
                          __func__,
                          ret_auth.auth_shard,
                          ho,
                          ret_auth.auth_oi)
           << dendl;

  return ret_auth;
#endif

  // TODO: very wrong
  return std::make_pair(
    maps.begin()->first,
    std::cref(maps.begin()->second.objects.find(hoid)->second));
}

void validate_object(const hobject_t &hoid, const scrub_map_set_t &maps)
{

  //auto auth_map_iter = select_auth_object(hoid, maps);
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
