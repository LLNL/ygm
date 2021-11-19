// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#undef NDEBUG
#include <ygm/comm.hpp>
#include <ygm/detail/ygm_ptr.hpp>

int main(int argc, char** argv) {
  ygm::comm world(&argc, &argv);

  //
  // Test Rank 0 async to all others
  {
    size_t               counter{};
    ygm::ygm_ptr<size_t> pcounter(&counter);
    world.barrier();
    if (world.rank0()) {
      for (int dest = 0; dest < world.size(); ++dest) {
        world.async(
            dest, [](auto pcounter) { (*pcounter)++; }, pcounter);
      }
    }
    world.barrier();
    ASSERT_RELEASE(counter == 1);
  }

  //
  // Test all ranks async to all others
  {
    size_t               counter{};
    ygm::ygm_ptr<size_t> pcounter(&counter);
    world.barrier();
    for (int dest = 0; dest < world.size(); ++dest) {
      world.async(
          dest, [](auto pcounter) { (*pcounter)++; }, pcounter);
    }
    world.barrier();
    ASSERT_RELEASE(counter == (size_t)world.size());
  }

  //
  // Test async_bcast
  {
    size_t               counter{};
    ygm::ygm_ptr<size_t> pcounter(&counter);
    world.barrier();
    if (world.rank0()) {
      world.async_bcast([](auto pcounter) { (*pcounter)++; }, pcounter);
    }

    world.barrier();
    ASSERT_RELEASE(counter == 1);
  }

  //
  // Test async_mcast
  {
    size_t               counter{};
    ygm::ygm_ptr<size_t> pcounter(&counter);
    world.barrier();
    if (world.rank0()) {
      std::vector<int> dests;
      for (int dest = 0; dest < world.size(); dest += 2) {
        dests.push_back(dest);
      }
      world.async_mcast(
          dests, [](auto pcounter) { (*pcounter)++; }, pcounter);
    }

    world.barrier();
    if (world.rank() % 2) {
      ASSERT_RELEASE(counter == 0);
    } else {
      ASSERT_RELEASE(counter == 1);
    }
  }

  //
  // Test reductions
  {
    auto max = world.all_reduce_max(size_t(world.rank()));
    ASSERT_RELEASE(max == (size_t)world.size() - 1);

    auto min = world.all_reduce_min(size_t(world.rank()));
    ASSERT_RELEASE(min == 0);

    auto sum = world.all_reduce_sum(size_t(world.rank()));
    ASSERT_RELEASE(sum ==
                   (((size_t)world.size() - 1) * (size_t)world.size()) / 2);

    size_t id  = world.rank();
    auto   red = world.all_reduce(id, [](size_t a, size_t b) {
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
    ASSERT_RELEASE(red2 == (size_t)world.size() - 1);
  }
  return 0;
}
