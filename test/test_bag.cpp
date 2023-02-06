// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#undef NDEBUG

#include <string>
#include <ygm/comm.hpp>
#include <ygm/container/bag.hpp>

int main(int argc, char **argv) {
  ygm::comm world(&argc, &argv);

  //
  // Test Rank 0 async_insert
  {
    ygm::container::bag<std::string> bbag(world);
    if (world.rank0()) {
      bbag.async_insert("dog");
      bbag.async_insert("apple");
      bbag.async_insert("red");
    }
    ASSERT_RELEASE(bbag.size() == 3);
  }

  //
  // Test all ranks async_insert
  {
    ygm::container::bag<std::string> bbag(world);
    bbag.async_insert("dog");
    bbag.async_insert("apple");
    bbag.async_insert("red");
    ASSERT_RELEASE(bbag.size() == 3 * (size_t)world.size());

    auto all_data = bbag.gather_to_vector(0);
    if (world.rank0()) {
      ASSERT_RELEASE(all_data.size() == 3 * (size_t)world.size());
    }
  }

  //
  // Test local_for_random_samples
  {
    int size           = world.size() * 8;
    int local_requests = 4;

    ygm::container::bag<int> bbag(world);

    if (world.rank0()) {
      for (int i(0); i < size; ++i) {
        bbag.async_insert(i);
      }
    }

    world.barrier();

    static int local_fulfilled(0);
    bbag.local_for_random_samples(
        local_requests, [&world](const auto &obj) { ++local_fulfilled; });

    world.barrier();

    ASSERT_RELEASE(local_requests == local_fulfilled);
  }
}