// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#undef NDEBUG
#include <ygm/comm.hpp>
#include <ygm/detail/ygm_ptr.hpp>

int main(int argc, char **argv) {
  ygm::comm world(&argc, &argv);

  //
  // Test Rank 0 async to all others
  {
    std::atomic<size_t> counter{};
    ygm::ygm_ptr<std::atomic<size_t>> pcounter(&counter);

    world.barrier(); // Needs a barrier to prevent a worker incrementing a
                     // counter before it exists
    if (world.rank() == 0) {
      for (int dest = 0; dest < world.size(); ++dest) {
        world.async(dest,
                    [](auto pcomm, int from, auto pcounter) { (*pcounter)++; },
                    pcounter);
      }
    }
    world.barrier();
    ASSERT_RELEASE(counter == 1);
  }

  //
  // Test all ranks async to all others
  {
    std::atomic<size_t> counter{};
    ygm::ygm_ptr<std::atomic<size_t>> pcounter(&counter);

    world.barrier();
    for (int dest = 0; dest < world.size(); ++dest) {
      world.async(dest,
                  [](auto pcomm, int from, auto pcounter) { (*pcounter)++; },
                  pcounter);
    }
    world.barrier();
    ASSERT_RELEASE(counter == world.size());
  }

  //
  // Test reductions
  {
    auto max = world.all_reduce_max(size_t(world.rank()));
    ASSERT_RELEASE(max == world.size() - 1);

    auto min = world.all_reduce_min(size_t(world.rank()));
    ASSERT_RELEASE(min == 0);

    auto sum = world.all_reduce_sum(size_t(world.rank()));
    ASSERT_RELEASE(sum == ((world.size() - 1) * world.size()) / 2);

    size_t id = world.rank();
    auto red = world.all_reduce(id, [](size_t a, size_t b) {
      if (a < b) {
        return a;
      } else {
        return b;
      }
    });
    ASSERT_RELEASE(red == 0);
    auto red2 = world.all_reduce(id, [](size_t a, size_t b) {
      if (a > b) {
        return a;
      } else {
        return b;
      }
    });
    ASSERT_RELEASE(red2 == world.size() - 1);
  }
  return 0;
}
