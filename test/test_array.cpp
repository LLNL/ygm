// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#undef NDEBUG

#include <ygm/comm.hpp>
#include <ygm/container/array.hpp>

int main(int argc, char **argv) {
  ygm::comm world(&argc, &argv);

  // Test async_put
  {
    int                        size = 64;
    ygm::container::array<int> arr(world, size);

    if (world.rank0()) {
      for (int i = 0; i < size; ++i) {
        arr.async_put(i, i);
      }
    }

    arr.for_all([](const auto index, const auto value) {
      ASSERT_RELEASE(index == value);
    });
  }

  // Test resize
  {
    int initial_size  = 32;
    int initial_value = 1;
    int final_size    = 64;
    int final_value   = 2;

    ygm::container::array<int> arr(world, initial_size, initial_value);

    arr.resize(final_size, final_value);

    arr.for_all([initial_size, initial_value, final_value](const auto index,
                                                           const auto value) {
      if (index < initial_size) {
        ASSERT_RELEASE(value == initial_value);
      } else {
        ASSERT_RELEASE(value == final_value);
      }
    });
  }

  // Test async_binary_op_update_value
  /*
{
int size = 32;

ygm::container::array<int> arr(world, size);

if (world.rank0()) {
for (int i = 0; i < size; ++i) {
  arr.async_put(i, i);
}
}

world.barrier();

for (int i = 0; i < size; ++i) {
arr.async_binary_op_update_value(i, 1, std::plus<int>());
}

arr.for_all([&world](const auto index, const auto value) {
ASSERT_RELEASE(value == index + world.size());
});
}
  */

  return 0;
}
