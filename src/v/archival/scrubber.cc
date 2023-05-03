/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "archival/scrubber.h"

#include "archival/logger.h"
#include "cloud_storage/lifecycle_marker.h"
#include "cloud_storage/partition_manifest.h"
#include "cloud_storage/remote.h"
#include "cloud_storage/remote_partition.h"
#include "cloud_storage/remote_segment.h"
#include "cloud_storage/topic_manifest.h"
#include "cloud_storage/tx_range_manifest.h"
#include "cluster/members_table.h"
#include "cluster/topic_table.h"
#include "cluster/topics_frontend.h"
#include "config/configuration.h"
#include "config/node_config.h"
#include "hashing/xx.h"
#include "vlog.h"

#include <seastar/coroutine/maybe_yield.hh>

namespace {
static constexpr std::string_view serde_extension = ".bin";
static constexpr std::string_view json_extension = ".json";

static constexpr auto partition_purge_timeout = 20s;
} // namespace

namespace archival {

using cloud_storage::download_result;
using cloud_storage::upload_result;

scrubber::scrubber(
  storage::api& local_storage,
  cloud_storage::remote& remote_storage,
  cluster::topic_table& tt,
  ss::sharded<cluster::topics_frontend>& tf,
  ss::sharded<cluster::members_table>& mt)
  : _storage(local_storage)
  , _api(remote_storage)
  , _topic_table(tt)
  , _topics_frontend(tf)
  , _members_table(mt)
  , _scrub_interval_secs(
      config::shard_local_cfg().cloud_storage_metadata_scrub_interval.bind())
  , _scrub_interval(update_interval()) {
    load_state();

    if (_scrub_interval_secs().has_value()) {
        _next_scrub = ss::lowres_clock::now() + _scrub_interval.next_duration();
    }

    _scrub_interval_secs.watch([this]() {
        _scrub_interval = update_interval();
        if (!config::shard_local_cfg()
               .cloud_storage_metadata_scrub_interval()
               .has_value()) {
            _next_scrub.reset();
        } else {
            _next_scrub = ss::lowres_clock::now()
                          + _scrub_interval.next_duration();
        }
    });
}

scrubber::interval_jitter_t scrubber::update_interval() {
    auto& interval_opt = _scrub_interval_secs();
    if (interval_opt.has_value()) {
        uint32_t jitter_secs = interval_opt.value().count()
                               * _scrub_interval_jitter_ratio;
        return interval_jitter_t(
          interval_opt.value(), std::chrono::seconds{jitter_secs});
    } else {
        return interval_jitter_t(std::chrono::seconds{0});
    }
}

ss::future<scrubber::purge_result> scrubber::purge_partition(
  const cluster::nt_lifecycle_marker& lifecycle_marker,
  model::ntp ntp,
  model::initial_revision_id remote_revision,
  retry_chain_node& parent_rtc) {
    retry_chain_node partition_purge_rtc(
      partition_purge_timeout, 100ms, &parent_rtc);
    retry_chain_logger ctxlog(archival_log, partition_purge_rtc);

    if (lifecycle_marker.config.is_read_replica()) {
        // Paranoia check: should never happen.
        // It never makes sense to have written a lifecycle marker that
        // has read replica flag set, something isn't right here, do not
        // delete.
        vlog(
          ctxlog.error,
          "Read replica mode set in tombstone on {}, refusing to purge",
          ntp);
        co_return purge_result{.status = purge_status::success, .ops = 0};
    }

    if (!lifecycle_marker.config.properties.remote_delete) {
        // Paranoia check: should never happen.
        // We should not have been called for a topic with remote delete
        // disabled, but double-check just in case.
        vlog(
          ctxlog.error,
          "Remote delete disabled in tombstone on {}, refusing to purge",
          ntp);
        co_return purge_result{.status = purge_status::success, .ops = 0};
    }

    auto collected = co_await collect_manifest_paths(
      bucket, ntp, remote_revision, partition_purge_rtc);
    if (!collected) {
        co_return purge_result{
          .status = purge_status::retryable_failure, .ops = 0};
    } else if (collected->empty()) {
        vlog(ctxlog.debug, "Nothing to purge for {}", ntp);
        co_return purge_result{.status = purge_status::success, .ops = 0};
    }

    const auto [manifests_to_purge, legacy_manifest_path]
      = collected->flatten();

    size_t ops_performed = 0;
    size_t permanent_failure = 0;
    for (auto rit = manifests_to_purge.rbegin();
         rit != manifests_to_purge.rend();
         ++rit) {
        auto format = cloud_storage::manifest_format::serde;
        if (std::string_view{*rit}.ends_with(json_extension)) {
            format = cloud_storage::manifest_format::json;
        }

        const auto local_res = co_await purge_manifest(
          bucket,
          ntp,
          remote_revision,
          remote_manifest_path{*rit},
          format,
          partition_purge_rtc);

        ops_performed += local_res.ops;

        switch (local_res.status) {
        case purge_status::retryable_failure:
            // Drop out when encountering a retry-able failure. These are often
            // back-offs from cloud storage, so it's wise to terminate the scrub
            // and retry later.
            vlog(
              ctxlog.info,
              "Retryable failures encountered while purging partition {}. Will "
              "retry ...",
              ntp);

            co_return purge_result{
              .status = purge_status::retryable_failure, .ops = ops_performed};
            break;
        case purge_status::permanent_failure:
            // Keep going when encountering a permanent failure. We might still
            // be able to purge subsequent manifests.
            ++permanent_failure;
            break;
        case purge_status::success:
            break;
        }
    }

    if (legacy_manifest_path) {
        vlog(
          ctxlog.debug,
          "Erasing legacy partition manifest {}",
          *legacy_manifest_path);
        const auto manifest_delete_result = co_await _api.delete_object(
          bucket,
          cloud_storage_clients::object_key(*legacy_manifest_path),
          partition_purge_rtc);
        if (manifest_delete_result != upload_result::success) {
            vlog(
              ctxlog.info,
              "Retryable failures encountered while purging partition legacy "
              "manifest at {}. Will "
              "retry ...",
              legacy_manifest_path.value());

            co_return purge_result{
              .status = purge_status::retryable_failure, .ops = ops_performed};
        }
    }

    if (permanent_failure > 0) {
        vlog(
          ctxlog.error,
          "Permanent failures encountered while purging partition {}. Giving "
          "up ...",
          ntp);

        co_return purge_result{
          .status = purge_status::permanent_failure, .ops = ops_performed};
    } else {
        vlog(
          ctxlog.info,
          "Finished erasing partition {} from object storage in {} requests",
          ntp,
          ops_performed);

        co_return purge_result{
          .status = purge_status::success, .ops = ops_performed};
    }
}

ss::future<std::optional<scrubber::collected_manifests>>
scrubber::collect_manifest_paths(
  const cloud_storage_clients::bucket_name& bucket,
  model::ntp ntp,
  model::initial_revision_id remote_revision,
  retry_chain_node& parent_rtc) {
    retry_chain_node collection_rtc(&parent_rtc);
    retry_chain_logger ctxlog(archival_log, collection_rtc);

    cloud_storage::partition_manifest manifest(ntp, remote_revision);
    auto path = manifest.get_manifest_path(
      cloud_storage::manifest_format::serde);

    std::string_view base_path{path().native()};

    vassert(
      base_path.ends_with(serde_extension)
        && base_path.length() > serde_extension.length(),
      "Generated manifest path should end in .bin");

    base_path.remove_suffix(serde_extension.length());
    auto list_result = co_await _api.list_objects(
      bucket,
      collection_rtc,
      cloud_storage_clients::object_key{std::filesystem::path{base_path}});

    if (list_result.has_error()) {
        vlog(
          ctxlog.warn,
          "Failed to collect manifests to scrub partition {}",
          ntp);

        co_return std::nullopt;
    }

    collected_manifests collected{};
    collected.spillover.reserve(list_result.value().contents.size());
    for (auto& item : list_result.value().contents) {
        std::string_view path{item.key};
        if (path.ends_with(".bin")) {
            collected.current_serde = std::move(item.key);
            continue;
        }

        if (path.ends_with(".json")) {
            collected.current_json = std::move(item.key);
            continue;
        }

        collected.spillover.push_back(std::move(item.key));
    }

    co_return collected;
}

ss::future<scrubber::purge_result> scrubber::purge_manifest(
  const cloud_storage_clients::bucket_name& bucket,
  model::ntp ntp,
  model::initial_revision_id remote_revision,
  remote_manifest_path manifest_key,
  cloud_storage::manifest_format format,
  retry_chain_node& parent_rtc) {
    retry_chain_node manifest_purge_rtc(&parent_rtc);
    retry_chain_logger ctxlog(archival_log, manifest_purge_rtc);

    purge_result result{.status = purge_status::success, .ops = 0};

    vlog(
      ctxlog.info,
      "Purging manifest at {} and its contents for partition {}",
      manifest_key(),
      ntp);

    cloud_storage::partition_manifest manifest(ntp, remote_revision);
    auto manifest_get_result = co_await _api.download_manifest(
      bucket, {format, manifest_key}, manifest, manifest_purge_rtc);

    if (manifest_get_result == download_result::notfound) {
        vlog(ctxlog.debug, "Partition manifest get {} not found", manifest_key);
        result.status = purge_status::permanent_failure;
        co_return result;
    } else if (manifest_get_result != download_result::success) {
        vlog(
          ctxlog.debug,
          "Partition manifest get {} failed: {}",
          manifest_key(),
          manifest_get_result);
        result.status = purge_status::retryable_failure;
        co_return result;
    }

    // A rough guess at how many ops will be involved in deletion, so that
    // we don't have to plumb ops-counting all the way down into object store
    // clients' implementations of plural object delete (different
    // implementations may do bigger/smaller batches).  This 1000 number
    // reflects the number of objects per S3 DeleteObjects.
    const auto estimate_delete_ops = std::max(
      static_cast<size_t>(manifest.size() / 1000), size_t{1});

    const auto erase_result = co_await cloud_storage::remote_partition::erase(
      _api, bucket, std::move(manifest), manifest_key, manifest_purge_rtc);

    result.ops += estimate_delete_ops;

    if (erase_result != cloud_storage::remote_partition::erase_result::erased) {
        vlog(
          ctxlog.debug,
          "One or more objects deletions failed for manifest {} of {}",
          manifest_key(),
          ntp);
        result.status = purge_status::retryable_failure;
        co_return result;
    }

    co_return result;
}

scrubber::global_position scrubber::get_global_position() {
    const auto& nodes = _members_table.local().nodes();
    auto self = config::node().node_id();

    // members_table doesn't store nodes in sorted container, so
    // we compose a sorted order first.
    auto node_ids = _members_table.local().node_ids();
    std::sort(node_ids.begin(), node_ids.end());

    uint32_t result = 0;
    uint32_t total = 0;

    // Iterate over node IDs earlier than ours, sum their core counts
    for (auto i : node_ids) {
        const auto cores = nodes.at(i).broker.properties().cores;
        if (i < self) {
            result += cores;
        }
        total += cores;
    }

    result += ss::this_shard_id();

    return global_position{.self = result, .total = total};
}

ss::future<housekeeping_job::run_result>
scrubber::run(retry_chain_node& parent_rtc, run_quota_t quota) {
    auto gate_holder = _gate.hold();

    run_result result{
      .status = run_status::skipped,
      .consumed = run_quota_t(0),
      .remaining = quota,
    };

    if (!_enabled) {
        co_return result;
    }

    const auto my_global_position = get_global_position();

    auto& bucket_config_property
      = cloud_storage::configuration::get_bucket_config();
    if (!bucket_config_property().has_value()) {
        // In general it is acceptable to have tiered storage disabled, but
        // it is a warning if someone has done this while there are still
        // lifecycle markers pending processing.
        if (!_topic_table.get_lifecycle_markers().empty()) {
            vlog(
              archival_log.error,
              "Lifecycle marker exists but cannot be purged because "
              "{} is not set.",
              bucket_config_property.name());
        }

        co_return result;
    } else {
        bucket = cloud_storage_clients::bucket_name{
          bucket_config_property().value()};
    }

    // Each step will internally check for remaining quota in `result`, so
    // we may call them all unconditionally.
    co_await complete_topic_erases(parent_rtc, my_global_position, result);
    co_await backward_scan(parent_rtc, my_global_position, result);

    co_return result;
}

// Match segment paths, including all suffixes
// e.g. '568bb543/kafka/example-topic/0_126/0-6-895-1-v1.log.2'
//      (also matches with any trailing string like .tx or .index)
const std::regex segment_path_regex{
  R"REGEX([a-z0-9]+\/(.+)/(.+)/(\d+)_(\d+)/.+\.log\.\d+.*)REGEX"};

const std::regex manifest_path_regex{R"REGEX(.*manifest.json*)REGEX"};

ss::future<> scrubber::complete_topic_erases(
  retry_chain_node& parent_rtc,
  scrubber::global_position my_global_position,
  run_result& result) {
    // TODO: grace period to let individual cluster::partitions finish
    // deleting their own partitions under normal circumstances.

    // run() should always have set bucket first
    vassert(
      bucket != cloud_storage_clients::bucket_name{},
      "Called without initializing bucket");

    // Take a copy, as we will iterate over it asynchronously
    cluster::topic_table::lifecycle_markers_t markers
      = _topic_table.get_lifecycle_markers();

    vlog(
      archival_log.info,
      "Running with {} quota, {} topic lifecycle markers",
      result.remaining,
      markers.size());
    for (auto& [nt_revision, marker] : markers) {
        // Double check the topic config is elegible for remote deletion
        if (!marker.config.properties.requires_remote_erase()) {
            vlog(
              archival_log.warn,
              "Dropping lifecycle marker {}, is not suitable for remote purge",
              marker.config.tp_ns);

            co_await _topics_frontend.local().purged_topic(nt_revision, 5s);
            continue;
        }

        // Check if the grace period has elapsed. The intent here is to
        // avoid races with `remote_partition::finalize`.
        const auto now = ss::lowres_system_clock::now();
        if (
          marker.timestamp.has_value()
          && now - marker.timestamp.value()
               < config::shard_local_cfg()
                   .cloud_storage_topic_purge_grace_period_ms()) {
            vlog(
              archival_log.debug,
              "Grace period for {} is still in effect for. Skipping scrub.",
              marker.config.tp_ns);

            continue;
        }

        // TODO: share work at partition granularity, not topic.  Requires
        // a feedback mechanism for the work done on partitions to be made
        // visible to the shard handling the total topic.

        // Map topics to shards based on simple hash, to distribute work
        // if there are many topics to clean up.
        incremental_xxhash64 inc_hash;
        inc_hash.update(nt_revision.nt.ns);
        inc_hash.update(nt_revision.nt.tp);
        inc_hash.update(nt_revision.initial_revision_id);
        uint32_t hash = static_cast<uint32_t>(inc_hash.digest() & 0xffffffff);

        if (my_global_position.self == hash % my_global_position.total) {
            vlog(
              archival_log.info,
              "Processing topic lifecycle marker {} ({} partitions)",
              marker.config.tp_ns,
              marker.config.partition_count);
            auto& topic_config = marker.config;

            // Persist a record that we have started purging: this is useful for
            // anything reading from the bucket that wants to distinguish
            // corruption from in-progress deletion.
            auto marker_r = co_await write_remote_lifecycle_marker(
              nt_revision,
              bucket,
              cloud_storage::lifecycle_status::purging,
              parent_rtc);
            if (marker_r != cloud_storage::upload_result::success) {
                vlog(
                  archival_log.warn,
                  "Failed to write lifecycle marker, not purging {}",
                  nt_revision);
                result.status = run_status::failed;
                co_return;
            }

            for (model::partition_id i = model::partition_id{0};
                 i < marker.config.partition_count;
                 ++i) {
                model::ntp ntp(nt_revision.nt.ns, nt_revision.nt.tp, i);

                if (result.quota_exhausted()) {
                    // Exhausted quota, drop out.
                    vlog(archival_log.debug, "Exhausted quota, dropping out");
                    co_return;
                }

                auto purge_r = co_await purge_partition(
                  marker, ntp, marker.initial_revision_id, parent_rtc);

                result.consume_quota(run_quota_t(purge_r.ops));

                if (purge_r.status == purge_status::success) {
                    result.status = run_status::ok;
                } else if (purge_r.status == purge_status::permanent_failure) {
                    // If we permanently fail to purge a partition, we pretend
                    // to succeed and proceed to clean up the tombstone, to
                    // avoid remote storage issues blocking us from cleaning
                    // up tombstones
                    result.status = run_status::ok;
                } else {
                    vlog(
                      archival_log.info,
                      "Failed to purge {}, will retry on next scrub",
                      ntp);
                    result.status = run_status::failed;
                    co_return;
                }
            }

            // At this point, all partition deletions either succeeded or
            // permanently failed: clean up the topic manifest and erase
            // the controller tombstone.
            auto topic_manifest_path
              = cloud_storage::topic_manifest::get_topic_manifest_path(
                topic_config.tp_ns.ns, topic_config.tp_ns.tp);
            vlog(
              archival_log.debug,
              "Erasing topic manifest {}",
              topic_manifest_path);
            retry_chain_node topic_manifest_rtc(5s, 1s, &parent_rtc);
            auto manifest_delete_result = co_await _api.delete_object(
              bucket,
              cloud_storage_clients::object_key(topic_manifest_path),
              topic_manifest_rtc);
            if (manifest_delete_result != upload_result::success) {
                vlog(
                  archival_log.info,
                  "Failed to erase topic manifest {}, will retry on next scrub",
                  nt_revision.nt);
                result.status = run_status::failed;
                co_return;
            }

            // Before purging the topic from the controller, write a permanent
            // lifecycle marker to object storage, enabling readers to
            // unambiguously understand that this topic is gone due to an
            // intentional deletion, and that any stray objects belonging to
            // this topic may be purged.
            marker_r = co_await write_remote_lifecycle_marker(
              nt_revision,
              bucket,
              cloud_storage::lifecycle_status::purged,
              parent_rtc);
            if (marker_r != cloud_storage::upload_result::success) {
                vlog(
                  archival_log.warn,
                  "Failed to write lifecycle marker, not purging {}",
                  nt_revision);
                result.status = run_status::failed;
                co_return;
            }

            // All topic-specific bucket contents are gone, we may erase
            // our controller tombstone.
            auto purge_result = co_await _topics_frontend.local().purged_topic(
              nt_revision, 5s);
            if (purge_result.ec != cluster::errc::success) {
                auto errc = cluster::make_error_code(purge_result.ec);
                // Just log: this will get retried next time the scrubber runs
                vlog(
                  archival_log.info,
                  "Failed to mark topic {} purged: {}, will retry on next "
                  "scrub",
                  nt_revision.nt,
                  errc.message());
            } else {
                vlog(archival_log.info, "Topic {} purge complete", nt_revision);
            }
        }
    }
}

/// Inner part of backward_scan, run for each prefix it wil:w
/// l inspect.
ss::future<> scrubber::backward_scan_prefix(
  std::string prefix,
  retry_chain_node& parent_rtc,
  scrubber::global_position pos,
  run_result& result) {
    // run() should always have set bucket first
    vassert(
      bucket != cloud_storage_clients::bucket_name{},
      "Called without initializing bucket");

    std::optional<ss::sstring> continuation_token;
    bool have_more_data = true;
    while (have_more_data) {
        retry_chain_node list_rtc(5s, 1s, &parent_rtc);
        auto list_result = co_await _api.list_objects(
          bucket,
          list_rtc,
          cloud_storage_clients::object_key{prefix},
          std::nullopt,
          std::nullopt,
          continuation_token,
          cloud_storage::remote::list_paginate::yes);
        if (list_result.has_error()) {
            // Scrubbing is best effort.  If we hit an error, drop out
            // as an implicit backoff.  This assumes that an object
            // listing can never have a persistent error: if it does,
            // that is likely to be a bug in the object storage backend.
            co_return;
        } else {
            auto listing = list_result.value();
            continuation_token = listing.next_continuation_token;
            have_more_data = listing.is_truncated;
            vlog(
              archival_log.debug,
              "backward_scan: {} items continuation token {} have_more_data={}",
              listing.contents.size(),
              continuation_token.has_value() ? continuation_token.value() : "",
              have_more_data);

            for (const auto& item : listing.contents) {
                vlog(
                  archival_log.trace,
                  "backward_scan: key {} {} {} {}",
                  item.key,
                  item.size_bytes,
                  item.etag,
                  item.last_modified);
                // TODO: resolve key to metadata and conditionally clean
                // up if it's an orphan
                std::match_results<const char*> match;
                auto matched = std::regex_match(
                  item.key.begin(), item.key.end(), match, segment_path_regex);
                if (!matched) {
                    // Ignore manifests.
                    // TOOD: we should delete partition manifests
                    // if they correspond to deleted topics, although
                    // if this happens then it's a sign that something
                    // went badly wrong with forward-deletion phase.
                    if (!std::regex_match(
                          item.key.begin(),
                          item.key.end(),
                          manifest_path_regex)) {
                        vlog(
                          archival_log.warn,
                          "Unexpected object '{}'",
                          item.key);
                    }
                    continue;
                }

                auto& ns = match[1];
                auto& tp = match[2];
                auto& p = match[3];
                auto initial_rev_str = match[4];
                vlog(
                  archival_log.trace,
                  "Found segment for {}/{}/{}_{}",
                  ns,
                  tp,
                  p,
                  initial_rev_str);

                model::initial_revision_id initial_rev;
                try {
                    initial_rev = model::initial_revision_id{
                      std::stoul(initial_rev_str, nullptr, 10)};
                } catch (const std::invalid_argument&) {
                    vlog(
                      archival_log.warn, "Malformed segment key {}", item.key);
                    continue;
                }

                // Does the topic still exist?
                auto tp_md_opt = _topic_table.get_topic_metadata_ref(
                  model::topic_namespace(model::ns{ns}, model::topic{tp}));

                if (tp_md_opt.has_value()) {
                    auto table_initial_rev
                      = tp_md_opt->get().get_remote_revision().value_or(
                        model::initial_revision_id(
                          tp_md_opt->get().get_revision()));
                    if (table_initial_rev == initial_rev) {
                        // The topic still exists, nothing to do
                        vlog(
                          archival_log.trace,
                          "Not purging {}, topic is still live",
                          item.key);
                        continue;
                    } else {
                        vlog(
                          archival_log.trace,
                          "Topic exists but with different revision {}!={} "
                          "for segment {}",
                          table_initial_rev,
                          initial_rev,
                          item.key);
                    }
                } else {
                    vlog(
                      archival_log.trace,
                      "Topic {}/{} not found in topics table",
                      ns,
                      tp);
                }

                // If we got this far, we are looking at a segment for
                // a topic that no longer exists in the topic table.

                // See if we have a cached lifecycle marker for this topic,
                // that would enable us to decide whether to purge the
                // segment.
                auto marker_key
                  = cloud_storage::remote_nt_lifecycle_marker::generate_key(
                    model::ns{ns}, model::topic{tp}, initial_rev);

                retry_chain_node get_marker_rtc(5s, 1s, &parent_rtc);
                iobuf marker_bytes;
                auto dl_result = co_await _api.download_object(
                  bucket, marker_key, marker_bytes, get_marker_rtc);
                if (dl_result == cloud_storage::download_result::notfound) {
                    // FIXME: Something more structured here, because it
                    // will be the case whenever we have a leftover orphan
                    // object from a pre-23.2 redpanda which has deleted
                    // topics without leaving behind lifecycle markers.
                    vlog(
                      archival_log.debug,
                      "Topic deleted but no lifecycle marker found at {} (for "
                      "segment {})",
                      marker_key,
                      item.key);
                    continue;
                } else if (
                  dl_result != cloud_storage::download_result::success) {
                    vlog(
                      archival_log.debug,
                      "Topic deleted but error fetching lifecycle marker "
                      "for {} ",
                      item.key);
                    continue;
                }

                // We have a lifecycle marker, we may decide whether to
                // purge this orphan segment.
                cloud_storage::remote_nt_lifecycle_marker lifecycle_marker;
                try {
                    lifecycle_marker = serde::from_iobuf<
                      cloud_storage::remote_nt_lifecycle_marker>(
                      std::move(marker_bytes));
                } catch (...) {
                    vlog(
                      archival_log.warn,
                      "Malformed lifecycle marker detected at {} ({})",
                      marker_key,
                      std::current_exception());
                    continue;
                }

                if (
                  lifecycle_marker.status
                  == cloud_storage::lifecycle_status::purged) {
                    retry_chain_node delete_rtc(5s, 1s, &parent_rtc);
                    vlog(
                      archival_log.info,
                      "Purging segment {}, topic lifecycle status is {}",
                      item.key,
                      lifecycle_marker.status);
                    co_await _api.delete_object(
                      bucket,
                      cloud_storage_clients::object_key{item.key},
                      delete_rtc);
                } else {
                    // TODO: More clear/structured log for the case where
                    // the topic is in offloaded state (i.e. we are going
                    // to keep seeing these objects indefinitely)
                    vlog(
                      archival_log.debug,
                      "Not purging segment {}, lifecycle status is {}",
                      item.key,
                      lifecycle_marker.status);
                }

                // We already have a scheduling point every 1000 keys
                // because we do I/O to make another listing request, but
                // because this is low priority background work, yield even
                // more frequently to avoid imposing even a 1000 key loop of
                // delay on other tasks.
                co_await ss::coroutine::maybe_yield();
            }
        }
    }
}

ss::future<> scrubber::backward_scan(
  retry_chain_node& parent_rtc,
  scrubber::global_position cluster_pos,
  run_result& result) {
    if (result.quota_exhausted()) {
        vlog(archival_log.trace, "Skipping backward scrub, quota exhausted");
        co_return;
    }

    // backward scrub is disabled
    if (!_scrub_interval_secs().has_value()) {
        vlog(archival_log.trace, "Skipping backward scrub, disabled");
        co_return;
    }

    if (_state.has_value() && _state.value().in_progress()) {
        if (_state->current_position == cluster_pos) {
            // Scrub in progress, resume
            vlog(
              archival_log.trace,
              "Resuming scrub from prefix '{}'",
              _state.value().current_prefix);
        } else {
            // Cluster shape changed, restart scrub.
            vlog(archival_log.debug, "backward_scan: restarting scrub");
            _state = shard_scrub_state{
              .begin_time = model::timestamp::now(),
              .current_position = cluster_pos};
        }
    } else {
        // Scrub not in progress, ensure we have scheduled next scrub
        if (!_next_scrub.has_value()) {
            _next_scrub = ss::lowres_clock::now()
                          + _scrub_interval.next_duration();
        }

        // Either drop out, or start scrub
        if (ss::lowres_clock::now() < _next_scrub.value()) {
            vlog(
              archival_log.trace,
              "Not yet time for a backward scan, next scan starts in {} "
              "seconds",
              std::chrono::duration_cast<std::chrono::seconds>(
                _next_scrub.value() - ss::lowres_clock::now()));
            co_return;
        } else {
            // Start scrub
            vlog(archival_log.trace, "Starting full metadata scrub...");
            _state = shard_scrub_state{
              .begin_time = model::timestamp::now(),
              .current_position = cluster_pos};
        }
    }

    // Each shard in the system scans a lexical range of the bucket: this
    // is implemented with prefixes to provide compatibility with non-S3
    // APIs like ABS, which do not implement StartAfter for listing.
    //
    // We share the work 256 ways based on the first digit of the
    // segment hash.  In principle, we could dynamically go up to 65536
    // shares if there are more than 256 shards in the system, but there is
    // little practical benefit to scrubbing concurrently from more than
    // 256 shards at a time.
    size_t total_shares = 256;
    if (cluster_pos.self >= total_shares) {
        co_return;
    }

    size_t shares_per_shard = std::max(
      total_shares / cluster_pos.total, size_t{1});
    size_t base = shares_per_shard * cluster_pos.self;
    size_t max = shares_per_shard * (cluster_pos.self + 1);
    if (cluster_pos.self == 255) {
        max = total_shares;
    }

    vlog(
      archival_log.debug,
      "backward_scan: prefixes {:01x} - {:01x} inclusive ({} shards, {} per "
      "shard)",
      base,
      max - 1,
      cluster_pos.total,
      shares_per_shard);

    for (size_t prefix_i = base; prefix_i < max; ++prefix_i) {
        ss::sstring prefix = fmt::format("{:02x}", prefix_i);

        if (
          _state.value().current_prefix.has_value()
          && prefix < _state.value().current_prefix.value()) {
            // We are resuming a scrub, and had already scanned this prefix.
            continue;
        }

        // Update state _before_ checking quota: that way we will pick up
        // at the right prefix on next call, if we do drop out for quota
        // exhaustion on this iteration.
        _state.value().current_prefix = prefix;
        co_await save_state();

        if (result.remaining <= run_quota_t(0)) {
            co_return;
        } else {
            // Model one prefix scan as one unit of work.
            result.consume_quota(run_quota_t{1});
        }

        vlog(archival_log.debug, "backward_scan: prefix '{}'", prefix);
        co_await backward_scan_prefix(prefix, parent_rtc, cluster_pos, result);
        vlog(archival_log.debug, "backward_scan: finished prefix '{}'", prefix);
    }

    vlog(archival_log.debug, "backward_scan: scrub complete");
    _state.value().end_time = model::timestamp::now();
    _state.value().current_prefix = std::nullopt;
    _next_scrub = ss::lowres_clock::now() + _scrub_interval.next_duration();
    co_await save_state();
}

ss::future<cloud_storage::upload_result>
scrubber::write_remote_lifecycle_marker(
  const cluster::nt_revision& nt_revision,
  cloud_storage_clients::bucket_name& bucket,
  cloud_storage::lifecycle_status status,
  retry_chain_node& parent_rtc) {
    retry_chain_node marker_rtc(5s, 1s, &parent_rtc);
    cloud_storage::remote_nt_lifecycle_marker remote_marker{
      .cluster_id = config::shard_local_cfg().cluster_id().value_or(""),
      .topic = nt_revision,
      .status = status,
    };
    auto marker_key = remote_marker.get_key();

    co_return co_await _api.upload_object(
      bucket,
      marker_key,
      serde::to_iobuf(std::move(remote_marker)),
      marker_rtc,
      _api.make_lifecycle_marker_tags(
        nt_revision.nt.ns, nt_revision.nt.tp, nt_revision.initial_revision_id),
      "remote_lifecycle_marker");
}

ss::future<> scrubber::stop() {
    vlog(archival_log.info, "Stopping ({})...", _gate.get_count());
    if (!_as.abort_requested()) {
        _as.request_abort();
    }
    co_await _gate.close();
    vlog(archival_log.info, "Stopped.");
}

void scrubber::interrupt() { _as.request_abort(); }

bool scrubber::interrupted() const { return _as.abort_requested(); }

void scrubber::set_enabled(bool e) { _enabled = e; }

void scrubber::acquire() { _holder = ss::gate::holder(_gate); }

void scrubber::release() { _holder.release(); }

ss::sstring scrubber::name() const { return "scrubber"; }

bool scrubber::collected_manifests::empty() const {
    return !current_serde.has_value() && !current_json.has_value()
           && spillover.empty();
}

scrubber::collected_manifests::flat_t scrubber::collected_manifests::flatten() {
    std::optional<ss::sstring> legacy_manifest_path;
    auto manifests_to_purge = std::move(spillover);

    if (current_serde.has_value()) {
        manifests_to_purge.push_back(std::move(current_serde.value()));
        legacy_manifest_path = std::move(current_json);
    } else if (current_json.has_value()) {
        manifests_to_purge.push_back(std::move(current_json.value()));
    }

    return {std::move(manifests_to_purge), std::move(legacy_manifest_path)};
}

static bytes scrub_state_key{"scrubber_state"};

void scrubber::load_state() {
    vassert(!_state.has_value(), "Called load_state but already have state");

    auto state_bytes_opt = _storage.kvs().get(
      storage::kvstore::key_space::cloud_storage, scrub_state_key);
    if (!state_bytes_opt.has_value()) {
        return;
    }

    _state = serde::from_iobuf<shard_scrub_state>(
      std::move(state_bytes_opt.value()));
}

ss::future<> scrubber::save_state() {
    if (!_state.has_value()) {
        co_return;
    }

    auto encoded = serde::to_iobuf(_state.value());
    co_await _storage.kvs().put(
      storage::kvstore::key_space::cloud_storage,
      scrub_state_key,
      std::move(encoded));
}

} // namespace archival
