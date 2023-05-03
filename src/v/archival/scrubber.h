/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "archival/types.h"
#include "cloud_storage/base_manifest.h"
#include "cloud_storage/fwd.h"
#include "cloud_storage/lifecycle_marker.h"
#include "cluster/fwd.h"
#include "cluster/types.h"
#include "storage/api.h"

#include <seastar/core/future.hh>

namespace archival {

/**
 * The scrubber is a global sharded service: it runs on all shards, and
 * decides internally which shard will scrub which ranges of objects
 * in object storage.
 */
class scrubber : public housekeeping_job {
public:
    explicit scrubber(
      storage::api&,
      cloud_storage::remote&,
      cluster::topic_table&,
      ss::sharded<cluster::topics_frontend>&,
      ss::sharded<cluster::members_table>&);

    ss::future<run_result>
    run(retry_chain_node& rtc, run_quota_t quota) override;

    void interrupt() override;

    bool interrupted() const override;

    ss::future<> stop() override;

    void set_enabled(bool) override;

    void acquire() override;
    void release() override;

    ss::sstring name() const override;

    void load_state();
    ss::future<> save_state();

private:
    enum class purge_status : uint8_t {
        success,

        // Unavailability of backend, timeouts, etc.  We should drop out
        // but the purge can be retried in a future housekeeping iteration.
        retryable_failure,

        // If we cannot possibly finish, for example because we got a 404
        // reading manifest.
        permanent_failure,
    };

    struct purge_result {
        purge_status status;
        size_t ops{0};
    };

    ss::future<purge_result> purge_partition(
      const cluster::nt_lifecycle_marker&,
      model::ntp,
      model::initial_revision_id,
      retry_chain_node& rtc);

    struct collected_manifests {
        using flat_t
          = std::pair<std::vector<ss::sstring>, std::optional<ss::sstring>>;

        std::optional<ss::sstring> current_serde;
        std::optional<ss::sstring> current_json;
        std::vector<ss::sstring> spillover;

        bool empty() const;
        [[nodiscard]] flat_t flatten();
    };

    ss::future<std::optional<collected_manifests>> collect_manifest_paths(
      const cloud_storage_clients::bucket_name&,
      model::ntp,
      model::initial_revision_id,
      retry_chain_node&);

    ss::future<purge_result> purge_manifest(
      const cloud_storage_clients::bucket_name&,
      model::ntp,
      model::initial_revision_id,
      remote_manifest_path,
      cloud_storage::manifest_format,
      retry_chain_node&);

    struct global_position
      : serde::envelope<
          global_position,
          serde::version<0>,
          serde::compat_version<0>> {
        uint32_t self;
        uint32_t total;
    };

    ss::future<cloud_storage::upload_result> write_remote_lifecycle_marker(
      const cluster::nt_revision&,
      cloud_storage_clients::bucket_name& bucket,
      cloud_storage::lifecycle_status status,
      retry_chain_node& parent_rtc);

    /**
     * Scrubs run infrequently and may take a long time: we must use a
     * persistent structure to carry this state across restarts.
     */
    struct shard_scrub_state
      : serde::envelope<
          shard_scrub_state,
          serde::version<0>,
          serde::compat_version<0>> {
        // On use of model::timestamp: these are not kafka timestamps, but using
        // model::timestamp is convenient because it has a more strictly defined
        // type/unit than generic time_point types.
        model::timestamp begin_time{model::timestamp::min()};
        std::optional<model::timestamp> end_time;

        // The role within the total space of shards that this shard had when it
        // started the scrub: this changes if shards are added/removed during
        // a scrub.  The position implies the list of prefixes that this
        // shard will scrub, current_prefix points to one of these.
        scrubber::global_position current_position;

        // If we are currently mid-scrub, remember the last prefix we started.
        // When we pick up again after restart, we will start from here.
        std::optional<ss::sstring> current_prefix;

        bool in_progress() { return !end_time.has_value(); }
    };

    /// Find our index out of all shards in the cluster
    global_position get_global_position();

    /// Process any outstanding lifecycle markers that require
    /// erasing a topic's remaining data.
    ss::future<> complete_topic_erases(
      retry_chain_node&, global_position, run_result& result);

    /// The backward (iterate over data, reconcile with metadata) portion of
    /// run()
    ss::future<>
    backward_scan(retry_chain_node&, global_position, run_result& result);

    ss::future<> backward_scan_prefix(
      std::string prefix,
      retry_chain_node&,
      global_position,
      run_result& result);

    // This is initialized early in run(), subsequent steps may assume it is
    // set.
    cloud_storage_clients::bucket_name bucket;

    ss::abort_source _as;
    ss::gate _gate;

    // A gate holder we keep on behalf of the housekeeping service, when
    // it acquire()s us.
    ss::gate::holder _holder;

    bool _enabled{true};

    // The state of ongoing scrub, or of the last completed scrub.  Only
    // up to date if _loaded is true.
    std::optional<shard_scrub_state> _state;

    std::optional<ss::lowres_clock::time_point> _next_scrub;

    /// Interval between metadata scrubs is jittered to avoid all shards
    /// hitting the object store at the same moment
    using interval_jitter_t
      = simple_time_jitter<ss::lowres_clock, ss::lowres_clock::duration>;
    static constexpr float _scrub_interval_jitter_ratio = 0.2;

    /// Call when config changes, set _scrub_interval to the result
    interval_jitter_t update_interval();

    storage::api& _storage;
    cloud_storage::remote& _api;
    cluster::topic_table& _topic_table;
    ss::sharded<cluster::topics_frontend>& _topics_frontend;
    ss::sharded<cluster::members_table>& _members_table;

    config::binding<std::optional<std::chrono::seconds>> _scrub_interval_secs;
    interval_jitter_t _scrub_interval;
};

} // namespace archival
