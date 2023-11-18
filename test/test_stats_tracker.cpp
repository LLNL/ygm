// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#undef NDEBUG

#include <ygm/comm.hpp>
#include <ygm/stats_tracker.hpp>

int main(int argc, char **argv) {
  ygm::comm world(&argc, &argv);

  // Test counter increment by 1
  {
    ygm::stats_tracker tracker(world);
    for (int i = 0; i < world.rank(); ++i) {
      tracker.increment_counter<"Rank">();
    }
    ASSERT_RELEASE(tracker.get_counter_local<"Rank">() == world.rank());
    ASSERT_RELEASE(tracker.get_counter_max<"Rank">() == world.size() - 1);
    ASSERT_RELEASE(tracker.get_counter_min<"Rank">() == 0);
    ASSERT_RELEASE(tracker.get_counter_sum<"Rank">() ==
                   (world.size() - 1) * world.size() / 2);
    ASSERT_RELEASE(tracker.get_counter_avg<"Rank">() ==
                   ((double)world.size() - 1) / 2);
  }

  // Test counter increment by variable
  {
    ygm::stats_tracker tracker(world);
    for (int i = 0; i < world.rank() + 1; ++i) {
      tracker.increment_counter<"Rank">(i);
    }
    ASSERT_RELEASE(tracker.get_counter_local<"Rank">() ==
                   world.rank() * (world.rank() + 1) / 2);
    ASSERT_RELEASE(tracker.get_counter_max<"Rank">() ==
                   world.size() * (world.size() - 1) / 2);
    ASSERT_RELEASE(tracker.get_counter_min<"Rank">() == 0);
    ASSERT_RELEASE(tracker.get_counter_sum<"Rank">() ==
                   (world.size() - 1) * world.size() * world.size() / 2 -
                       (world.size() - 1) * world.size() *
                           (2 * world.size() - 1) / 6);
    // This should be safe for world.size() < \approx 2^18...
    ASSERT_RELEASE(
        tracker.get_counter_avg<"Rank">() ==
        ((world.size() - 1) * world.size() * world.size() / 2 -
         (world.size() - 1) * world.size() * (2 * world.size() - 1) / 6) /
            ((double)world.size()));
  }

  // Test timer starting and stopping
  {
    ygm::stats_tracker tracker(world);
    std::vector<int>   my_vec;
    tracker.start_timer<"outer_timer">();
    for (int i = 0; i < 10 * (world.rank() + 1); ++i) {
      tracker.start_timer<"inner_timer">();
      my_vec.push_back(i);
      tracker.stop_timer<"inner_timer">();
    }
    tracker.stop_timer<"outer_timer">();

    // Check inner vs. outer timers
    ASSERT_RELEASE(tracker.get_time_local<"outer_timer">() >=
                   tracker.get_time_local<"inner_timer">());
    ASSERT_RELEASE(tracker.get_time_max<"outer_timer">() >=
                   tracker.get_time_max<"inner_timer">());
    ASSERT_RELEASE(tracker.get_time_min<"outer_timer">() >=
                   tracker.get_time_min<"inner_timer">());
    ASSERT_RELEASE(tracker.get_time_sum<"outer_timer">() >=
                   tracker.get_time_sum<"inner_timer">());
    ASSERT_RELEASE(tracker.get_time_avg<"outer_timer">() >=
                   tracker.get_time_avg<"inner_timer">());

    // Check ordering between local, min, max, avg, and sum
    ASSERT_RELEASE(tracker.get_time_local<"outer_timer">() >=
                   tracker.get_time_min<"outer_timer">());
    ASSERT_RELEASE(tracker.get_time_local<"outer_timer">() <=
                   tracker.get_time_max<"outer_timer">());
    ASSERT_RELEASE(tracker.get_time_local<"outer_timer">() <=
                   tracker.get_time_sum<"outer_timer">());
    ASSERT_RELEASE(tracker.get_time_min<"outer_timer">() <=
                   tracker.get_time_avg<"outer_timer">());
    ASSERT_RELEASE(tracker.get_time_avg<"outer_timer">() <=
                   tracker.get_time_max<"outer_timer">());
    ASSERT_RELEASE(tracker.get_time_max<"outer_timer">() * world.size() <=
                   tracker.get_time_sum<"outer_timer">());
  }
  return 0;
}
