// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/osd/scrub/scrub_validator.h"
#include "osd/ECUtil.h"

namespace crimson::osd::scrub {

/* Notes on classic flow:
 *
 * Scrub validation logic mostly lives in osd/scrubber/scrub_backend.*
 *
 * Main entry point to the validation process is PGScrubber::maps_compare_n_cleanup()
 * which calls ScrubBackend::scrub_compare_maps.
 *
 * Summary of ScrubBackend validation methods:
 * - merge_to_authoritative: generates authoritative_set, union of all map objects
 * - update_authoritative: populates m_auth_peers and m_cleaned_meta_map based on
 *   authoritative (map)
 * - compare_smaps/compare_object_in_maps: selects auth, compares maps against auth
 *   - select_auth_object: selects authoritative object
 *   - match_in_shards: compares each shard against auth
 *     - compare_object_details: logic for comparing
 */

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

struct shard_evaluation_t {
  shard_info_wrapper shard_info;

  std::optional<object_info_t> object_info;
  std::optional<SnapSet> snapset;
  std::optional<ECUtil::HashInfo> hinfo;

  bool from_primary{false};

  size_t omap_keys{0};
  size_t omap_bytes{0};

  bool has_errors() const {
    return shard_info.has_errors();
  }

  std::weak_ordering operator<=>(const shard_evaluation_t &rhs) const {
    if (has_errors() && !rhs.has_errors()) {
      return std::weak_ordering::greater;
    } else if (!has_errors() && rhs.has_errors()) {
      return std::weak_ordering::less;
    }


    if (from_primary && !rhs.from_primary) {
      return std::weak_ordering::less;
    } else if (!from_primary && rhs.from_primary) {
      return std::weak_ordering::greater;
    }
    return std::weak_ordering::equivalent;
  }
};
shard_evaluation_t evaluate_object_shard(
  const chunk_validation_policy_t &policy,
  const hobject_t &oid,
  pg_shard_t from,
  const ScrubMap::object *maybe_obj)
{
  shard_evaluation_t ret;
  if (from == policy.primary) {
    ret.from_primary = true;
  }
  if (!maybe_obj || maybe_obj->negative) {
    ceph_assert(!maybe_obj->negative); // impossible since chunky scrub was introduced
    ret.shard_info.set_missing();
    return ret;
  }

  auto &obj = *maybe_obj;
  /* We are ignoring ScrubMap::object::large_omap_object*, object_omap_* is all the
   * info we need */
  ret.omap_keys = obj.object_omap_keys;
  ret.omap_bytes = obj.object_omap_bytes;
  
  ret.shard_info.set_object(obj);

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

  if (ret.object_info &&
      obj.size != policy.logical_to_ondisk_size(ret.object_info->size)) {
    ret.shard_info.set_size_mismatch_info();
  }

  if (oid.is_head()) {
    auto xiter = obj.attrs.find(SS_ATTR);
    if (xiter == obj.attrs.end()) {
      ret.shard_info.set_snapset_missing();
    } else {
      bufferlist bl;
      bl.push_back(xiter->second);
      ret.snapset = SnapSet{};
      try {
	auto bliter = bl.cbegin();
	::decode(*(ret.snapset), bliter);
      } catch (...) {
	ret.shard_info.set_snapset_corrupted();
	ret.snapset = std::nullopt;
      }
    }
  }

  if (policy.is_ec()) {
    auto xiter = obj.attrs.find(ECUtil::get_hinfo_key());
    if (xiter == obj.attrs.end()) {
      ret.shard_info.set_hinfo_missing();
    } else {
      bufferlist bl;
      bl.push_back(xiter->second);
      ret.hinfo = ECUtil::HashInfo{};
      try {
	auto bliter = bl.cbegin();
	decode(*(ret.hinfo), bliter);
      } catch (...) {
	ret.shard_info.set_hinfo_corrupted();
	ret.hinfo = std::nullopt;
      }
    }
  }

  if (!policy.is_ec() && ret.object_info) {
    if (ret.shard_info.data_digest_present &&
	ret.object_info->is_data_digest() &&
	(ret.object_info->data_digest != ret.shard_info.data_digest)) {
      ret.shard_info.set_data_digest_mismatch_info();
    }
    if (ret.shard_info.omap_digest_present &&
	(ret.object_info->omap_digest != ret.shard_info.omap_digest)) {
      ret.shard_info.set_omap_digest_mismatch_info();
    }
  }

  return ret;
}

librados::obj_err_t compare_candidate_to_authoritative(
  const chunk_validation_policy_t &policy,
  const hobject_t &oid,
  const shard_evaluation_t &auth,
  const shard_evaluation_t &cand)
{
  using namespace librados;
  obj_err_t ret;

  if (cand.shard_info.has_shard_missing()) {
    return ret;
  }

  const auto &auth_si = auth.shard_info;
  const auto &cand_si = cand.shard_info;

  if (auth_si.data_digest != cand_si.data_digest) {
    ret.errors &= obj_err_t::DATA_DIGEST_MISMATCH;
  }

  if (auth_si.omap_digest != cand_si.omap_digest) {
    ret.errors &= obj_err_t::OMAP_DIGEST_MISMATCH;
  }

  {
    auto aiter = auth_si.attrs.find(OI_ATTR);
    ceph_assert(aiter != auth_si.attrs.end());

    auto citer = cand_si.attrs.find(OI_ATTR);
    if (citer == cand_si.attrs.end() ||
	aiter->second.contents_equal(citer->second)) {
      ret.errors &= obj_err_t::SNAPSET_INCONSISTENCY;
    }
  }

  if (oid.is_head()) {
    auto aiter = auth_si.attrs.find(SS_ATTR);
    ceph_assert(aiter != auth_si.attrs.end());

    auto citer = cand_si.attrs.find(SS_ATTR);
    if (citer == cand_si.attrs.end() ||
	aiter->second.contents_equal(citer->second)) {
      ret.errors &= obj_err_t::OBJECT_INFO_INCONSISTENCY;
    }
  }

  if (policy.is_ec()) {
    auto aiter = auth_si.attrs.find(ECUtil::get_hinfo_key());
    ceph_assert(aiter != auth_si.attrs.end());

    auto citer = cand_si.attrs.find(ECUtil::get_hinfo_key());
    if (citer == cand_si.attrs.end() ||
	aiter->second.contents_equal(citer->second)) {
      ret.errors &= obj_err_t::HINFO_INCONSISTENCY;
    }
  }

  if (auth_si.size != cand_si.size) {
    ret.errors &= obj_err_t::SIZE_MISMATCH;
  }

  auto is_sys_attr = [&policy](const auto &str) {
    return str == OI_ATTR || str == SS_ATTR ||
      (policy.is_ec() && str == ECUtil::get_hinfo_key());
  };
  for (auto aiter = auth_si.attrs.begin(); aiter != auth_si.attrs.end(); ++aiter) {
    if (is_sys_attr(aiter->first)) continue;

    auto citer = cand_si.attrs.find(aiter->first);
    if (citer == cand_si.attrs.end()) {
      ret.errors &= obj_err_t::ATTR_NAME_MISMATCH;
    } else if (!aiter->second.contents_equal(citer->second)) {
      ret.errors &= obj_err_t::ATTR_VALUE_MISMATCH;
    }
  }
  if (std::any_of(
	cand_si.attrs.begin(), cand_si.attrs.end(),
	[&is_sys_attr, &auth_si](auto &p) {
	  return !is_sys_attr(p.first) &&
	    auth_si.attrs.find(p.first) == auth_si.attrs.end();
	})) {
    ret.errors &= obj_err_t::ATTR_NAME_MISMATCH;
  }

  return ret;
}

struct object_evaluation_t {
  std::optional<inconsistent_obj_wrapper> inconsistency;
  std::optional<object_info_t> object_info;
  std::optional<SnapSet> snapset;

  size_t omap_keys{0};
  size_t omap_bytes{0};
};
object_evaluation_t evaluate_object(
  const chunk_validation_policy_t &policy,
  const hobject_t &hoid,
  const scrub_map_set_t &maps)
{
  ceph_assert(maps.size() > 0);
  using evaluation_vec_t = std::vector<std::pair<pg_shard_t, shard_evaluation_t>>;
  evaluation_vec_t shards;
  std::transform(
    maps.begin(), maps.end(),
    std::inserter(shards, shards.end()),
    [&hoid, &policy](const auto &item) -> evaluation_vec_t::value_type {
      const auto &[shard, scrub_map] = item;
      auto miter = scrub_map.objects.find(hoid);
      auto maybe_shard = miter == scrub_map.objects.end() ?
	nullptr : &(miter->second);
      return
	std::make_pair(
	  shard,
	  evaluate_object_shard(policy, hoid, shard, maybe_shard));
    });
  
  std::sort(shards.begin(), shards.end());

  auto &[auth_shard, auth_eval] = shards.front();

  object_evaluation_t ret;
  inconsistent_obj_wrapper iow{hoid};
  if (!auth_eval.has_errors()) {
    ret.object_info = auth_eval.object_info;
    ret.omap_keys = auth_eval.omap_keys;
    ret.omap_bytes = auth_eval.omap_bytes;
    ret.snapset = auth_eval.snapset;
    auth_eval.shard_info.authoritative = true;
    if (auth_eval.object_info->size > policy.max_object_size) {
      iow.set_size_too_large();
    }
    auth_eval.shard_info.selected_oi = true;
    std::for_each(
      shards.begin() + 1, shards.end(),
      [&policy, &hoid, &auth_eval, &iow](auto &p) {
	auto err = compare_candidate_to_authoritative(
	  policy, hoid, auth_eval, p.second);
	iow.merge(err);
	if (!err.errors && !p.second.has_errors()) {
	  p.second.shard_info.authoritative = true;
	}
      });
  }

  if (iow.errors ||
      std::any_of(shards.begin(), shards.end(),
		  [](auto &p) { return p.second.has_errors(); })) {
    for (auto &[source, eval] : shards) {
      iow.shards.emplace(
	librados::osd_shard_t{source.osd, source.shard},
	eval.shard_info);
      iow.union_shards.errors |= eval.shard_info.errors;
    }
    if (auth_eval.object_info) {
      iow.version = auth_eval.object_info->version.version;
    }
    ret.inconsistency = iow;
  }
  return ret;
}

using clone_meta_list_t = std::list<std::pair<hobject_t, object_info_t>>;
std::optional<inconsistent_snapset_wrapper> evaluate_snapset(
  const hobject_t &hoid,
  const std::optional<SnapSet> &maybe_snapset,
  const clone_meta_list_t &clones)
{
  /* inconsistent_snapset_t has several error codes that seem to pertain to
   * specific objects rather than to the snapset specifically.  I'm choosing
   * to ignore those for now TODOSAM */
  inconsistent_snapset_wrapper ret;
  if (!maybe_snapset) {
    ret.set_headless();
    return ret;
  }
  const auto &snapset = *maybe_snapset;

  auto ss_clone_size_iter = snapset.clone_size.begin();
  auto clone_iter = clones.begin();
  for (auto ss_clone_id : snapset.clones) {
    for (; clone_iter != clones.end() &&
	   clone_iter->first.snap < ss_clone_id;
	 ++clone_iter) {
      ret.set_clone(clone_iter->first.snap);
    }

    if (clone_iter == clones.end() ||
	clone_iter->first.snap != ss_clone_id) {
      ret.set_clone_missing(clone_iter->first.snap);
    }

    for (; ss_clone_size_iter != snapset.clone_size.end() &&
	   ss_clone_size_iter->first < ss_clone_id;
	 ++ss_clone_size_iter) {
      // TODOSAM: stray clone_size entry, not really an accurate code
      ret.set_size_mismatch();
    }

    if (clone_iter != clones.end() &&
	clone_iter->first.snap == ss_clone_id &&
	ss_clone_size_iter != snapset.clone_size.end() &&
	ss_clone_size_iter->first == ss_clone_id) {
      if (clone_iter->second.size != ss_clone_size_iter->second) {
	ret.set_size_mismatch();
      }
    }
  }
  if (ret.errors) {
    return ret;
  } else {
    return std::nullopt;
  }
}

void add_object_to_stats(
  const chunk_validation_policy_t &policy,
  const object_evaluation_t &eval,
  object_stat_sum_t *out)
{
  auto &ss = eval.snapset;
  if (!eval.object_info) {
    return;
  }
  auto &oi = *eval.object_info;
  ceph_assert(out);
  if (ss) {
    out->num_bytes += oi.size;
    for (auto clone : ss->clones) {
      out->num_bytes += ss->get_clone_bytes(clone);
      out->num_object_clones++;
    }
    if (oi.is_whiteout()) {
      out->num_whiteouts++;
    } else {
      out->num_objects++;
    }
  }
  if (oi.is_dirty()) {
    out->num_objects_dirty++;
  }
  if (oi.is_cache_pinned()) {
    out->num_objects_pinned++;
  }
  if (oi.has_manifest()) {
    out->num_objects_manifest++;
  }
  
  if (eval.omap_keys > 0) {
    out->num_objects_omap++;
  }
  out->num_omap_keys += eval.omap_keys;
  out->num_omap_bytes += eval.omap_bytes;

  if (oi.soid.nspace == policy.hitset_namespace) {
    out->num_objects_hit_set_archive++;
    out->num_bytes_hit_set_archive += oi.size;
  }

  if (eval.omap_keys > policy.omap_key_limit ||
      eval.omap_bytes > policy.omap_bytes_limit) {
    out->num_large_omap_objects++;
  }
}

chunk_result_t validate_chunk(
  const chunk_validation_policy_t &policy, const scrub_map_set_t &in)
{
  chunk_result_t ret;

  const std::set<hobject_t> object_set = get_object_set(in);
  
  std::list<std::pair<hobject_t, SnapSet>> heads;
  clone_meta_list_t clones;
  for (const auto &oid: object_set) {
    object_evaluation_t eval = evaluate_object(policy, oid, in);
    add_object_to_stats(policy, eval, &ret.stats);
    if (eval.inconsistency) {
      ret.object_errors.push_back(*eval.inconsistency);
    }
    if (oid.is_head()) {
      /* We're only going to consider the head object as "existing" if
       * evaluate_object was able to find a sensbile, authoritative copy
       * complete with snapset */
      if (eval.snapset) {
	heads.emplace_back(oid, *eval.snapset);
      }
    } else {
      /* We're only going to consider the clone object as "existing" if
       * evaluate_object was able to find a sensbile, authoritative copy
       * complete with an object_info */
      if (eval.object_info) {
	clones.emplace_back(oid, *eval.object_info);
      }
    }
  }

  const hobject_t max_oid = hobject_t::get_max();
  while (heads.size() || clones.size()) {
    const hobject_t &next_head = heads.size() ? heads.front().first : max_oid;
    const hobject_t &next_clone = clones.size() ? clones.front().first : max_oid;
    hobject_t head_to_process = std::min(next_head, next_clone).get_head();

    clone_meta_list_t clones_to_process;
    auto clone_iter = clones.begin();
    while (clone_iter != clones.end() && clone_iter->first < head_to_process)
      ++clone_iter;
    clones_to_process.splice(
      clones_to_process.end(), clones, clones.begin(), clone_iter);

    const auto head_meta = [&]() -> std::optional<SnapSet> {
      if (head_to_process == next_head) {
	auto ret = std::move(heads.front().second);
	heads.pop_front();
	return ret;
      } else {
	return std::nullopt;
      }
    }();

    if (auto result = evaluate_snapset(
	  head_to_process, head_meta, clones_to_process); result) {
      ret.snapset_errors.push_back(*result);
    }
  }

  for (const auto &i: ret.object_errors) {
    ret.stats.num_shallow_scrub_errors += i.has_shallow_errors();
    ret.stats.num_deep_scrub_errors += i.has_deep_errors();
  }
  ret.stats.num_scrub_errors = ret.stats.num_shallow_scrub_errors +
    ret.stats.num_deep_scrub_errors;

  return ret;
}

}
