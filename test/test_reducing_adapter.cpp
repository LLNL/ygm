// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#undef NDEBUG

#include <string>
#include <ygm/comm.hpp>
#include <ygm/container/array.hpp>
#include <ygm/container/map.hpp>
#include <ygm/container/reducing_adapter.hpp>

int main(int argc, char **argv) {
  ygm::comm world(&argc, &argv);

  //
  // Test reducing_adapter on ygm::map
  {
    ygm::container::map<std::string, int> test_map(world);

    auto reducing_map = ygm::container::make_reducing_adapter(
        test_map,
        [](const int &a, const int &b) { return std::max<int>(a, b); });

    int num_reductions = 6;
    for (int i = 0; i < num_reductions; ++i) {
      reducing_map.async_reduce("max", i);
    }

    world.barrier();

    test_map.for_all([&num_reductions](const auto &key, const auto &value) {
      if (key == "max") {
        ASSERT_RELEASE(value == num_reductions - 1);
      } else {
        ASSERT_RELEASE(false);
      }
    });
  }

  //
  // Test reducing_adapter on ygm::array
  {
    ygm::container::array<int> test_array(world, 10);

    auto reducing_array = ygm::container::make_reducing_adapter(
        test_array,
        [](const int &a, const int &b) { return std::max<int>(a, b); });

    int num_reductions = 6;
    for (int i = 0; i < num_reductions; ++i) {
      reducing_array.async_reduce(0, i);
    }

    test_array.for_all([&num_reductions](const auto &index, const auto &value) {
      if (index == 0) {
        ASSERT_RELEASE(value == num_reductions - 1);
      } else {
        ASSERT_RELEASE(value == 0);
      }
    });
  }

  return 0;
}
