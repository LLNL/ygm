// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#undef NDEBUG

#include <string>
#include <ygm/comm.hpp>
#include <ygm/container/map.hpp>
#include <ygm/container/reducing_adapter.hpp>

int main(int argc, char **argv) {
  ygm::comm world(&argc, &argv);

  {
    ygm::container::map<std::string, int> test_map(world);

    auto test_reducing_map = ygm::container::make_reducing_adapter(
        test_map,
        [](const int &a, const int &b) { return std::max<int>(a, b); });

    int num_reductions = 6;
    for (int i = 0; i < num_reductions; ++i) {
      test_reducing_map.async_reduce("max", i);
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

  return 0;
}
