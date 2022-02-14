/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage/remote_segment_index.h"
#include "random/generators.h"
#include "vlog.h"

#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>

#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test.hpp>

#include <stdexcept>

using namespace cloud_storage;

BOOST_AUTO_TEST_CASE(remote_segment_index_search_test) {
    // This value is a power of two - 1 on purpose. This way we
    // will read from the compressed part and from the buffer of
    // recent values. This is because the row widht is 16 and the
    // buffer has 16 elements. 1024 is 64 rows and 1023 is 63
    // rows + almost full buffer.
    size_t segment_num_batches = 1023;
    model::offset segment_base_rp_offset{1234};
    model::offset segment_base_kaf_offset{1210};

    std::vector<model::offset> rp_offsets;
    std::vector<model::offset> kaf_offsets;
    std::vector<size_t> file_offsets;
    int64_t rp = segment_base_rp_offset();
    int64_t kaf = segment_base_kaf_offset();
    size_t fpos = 0;
    bool is_config = false;
    for (size_t i = 0; i < segment_num_batches; i++) {
        if (!is_config) {
            rp_offsets.push_back(model::offset(rp));
            kaf_offsets.push_back(model::offset(kaf));
            file_offsets.push_back(fpos);
        }
        // The test queries every element using the key that matches the element
        // exactly and then it queries the element using the key which is
        // smaller than the element. In order to do this we need a way to
        // guarantee that the distance between to elements in the sequence is at
        // least 2, so we can decrement the key safely.
        auto batch_size = random_generators::get_int(2, 100);
        is_config = random_generators::get_int(20) == 0;
        rp += batch_size;
        kaf += is_config ? batch_size - 1 : batch_size;
        fpos += random_generators::get_int(1, 1000);
    }

    offset_index index(segment_base_rp_offset, segment_base_kaf_offset, 0U);
    model::offset last;
    model::offset klast;
    size_t flast;
    for (size_t i = 0; i < rp_offsets.size(); i++) {
        index.add(rp_offsets.at(i), kaf_offsets.at(i), file_offsets.at(i));
        last = rp_offsets.at(i);
        klast = kaf_offsets.at(i);
        flast = file_offsets.at(i);
    }

    // Query element before the first one
    auto opt_first = index.find_rp_offset(
      segment_base_rp_offset - model::offset(1));
    BOOST_REQUIRE(!opt_first.has_value());

    auto kopt_first = index.find_kaf_offset(
      segment_base_kaf_offset - model::offset(1));
    BOOST_REQUIRE(!kopt_first.has_value());

    for (unsigned ix = 0; ix < rp_offsets.size(); ix++) {
        auto opt = index.find_rp_offset(rp_offsets[ix] + model::offset(1));
        auto [rp, kaf, fpos] = *opt;
        BOOST_REQUIRE_EQUAL(rp, rp_offsets[ix]);
        BOOST_REQUIRE_EQUAL(kaf, kaf_offsets[ix]);
        BOOST_REQUIRE_EQUAL(fpos, file_offsets[ix]);

        auto kopt = index.find_kaf_offset(kaf_offsets[ix] + model::offset(1));
        BOOST_REQUIRE_EQUAL(kopt->rp_offset, rp_offsets[ix]);
        BOOST_REQUIRE_EQUAL(kopt->kaf_offset, kaf_offsets[ix]);
        BOOST_REQUIRE_EQUAL(kopt->file_pos, file_offsets[ix]);
    }

    // Query after the last element
    auto opt_last = index.find_rp_offset(last + model::offset(1));
    auto [rp_last, kaf_last, file_last] = *opt_last;
    BOOST_REQUIRE_EQUAL(rp_last, last);
    BOOST_REQUIRE_EQUAL(kaf_last, klast);
    BOOST_REQUIRE_EQUAL(file_last, flast);

    auto kopt_last = index.find_kaf_offset(klast + model::offset(1));
    BOOST_REQUIRE_EQUAL(kopt_last->rp_offset, last);
    BOOST_REQUIRE_EQUAL(kopt_last->kaf_offset, klast);
    BOOST_REQUIRE_EQUAL(kopt_last->file_pos, flast);
}
