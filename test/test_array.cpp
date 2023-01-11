// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#undef NDEBUG

#include <ygm/comm.hpp>
#include <ygm/container/array.hpp>

#include <cmath>

int main(int argc, char **argv) {
  ygm::comm world(&argc, &argv);

  // Test async_set
  {
    int                        size = 64;
    ygm::container::array<int> arr(world, size);

    if (world.rank0()) {
      for (int i = 0; i < size; ++i) {
        arr.async_set(i, i);
      }
    }

    arr.for_all([](auto iv_pair) {
      auto [index, value] = iv_pair;
      ASSERT_RELEASE(index == value);
    });
  }

  // Test assignment in for_all
  {
    int                        size = 64;
    ygm::container::array<int> arr(world, size);

    // This shows that even if the pair is const, the value_type reference is
    // still assignable.
    arr.for_all([](const auto &iv_pair) {
      auto [index, value] = iv_pair;
      value               = index;
    });

    arr.for_all([](auto &iv_pair) {
      auto [index, value] = iv_pair;
      ASSERT_RELEASE(index == value);
    });
  }

  // Test async_binary_op_update_value
  {
    int size = 32;

    ygm::container::array<int> arr(world, size);

    if (world.rank0()) {
      for (int i = 0; i < size; ++i) {
        arr.async_set(i, i);
      }
    }

    world.barrier();

    for (int i = 0; i < size; ++i) {
      arr.async_binary_op_update_value(i, 2, std::plus<int>());
    }

    arr.for_all([&world](const auto iv_pair) {
      auto [index, value] = iv_pair;
      ASSERT_RELEASE(value == index + 2 * world.size());
    });
  }

  // Test async_bit_xor
  {
    int size = 64;

    ygm::container::array<int> arr(world, size);

    if (world.rank0()) {
      for (int i = 0; i < size; ++i) {
        arr.async_set(i, i);
      }
    }

    world.barrier();

    for (int i = 0; i < size; ++i) {
      arr.async_bit_xor(i, world.rank());
    }

    arr.for_all([&world](auto iv_pair) {
      auto [index, value] = iv_pair;
      int cumulative_xor;
      switch ((world.size() - 1) % 4) {
        case 0:
          cumulative_xor = world.size() - 1;
          break;
        case 1:
          cumulative_xor = 1;
          break;
        case 2:
          cumulative_xor = world.size();
          break;
        case 3:
          cumulative_xor = 0;
          break;
      }
      ASSERT_RELEASE(value == index ^ cumulative_xor);
    });
  }

  // Test async_increment
  {
    int size = 64;

    ygm::container::array<int> arr(world, size);

    if (world.rank0()) {
      for (int i = 0; i < size; ++i) {
        arr.async_set(i, i);
      }
    }

    world.barrier();

    for (int i = 0; i < size; ++i) {
      arr.async_increment(i);
    }

    arr.for_all([&world](auto iv_pair) {
      auto [index, value] = iv_pair;
      ASSERT_RELEASE(value == index + world.size());
    });
  }

  // Test general async_visit
  {
    int size = 64;

    ygm::container::array<int> arr(world, size);

    if (world.rank0()) {
      for (int i = 0; i < size; ++i) {
        arr.async_set(i, i);
      }
    }

    world.barrier();

    auto square_visitor = [](auto iv_pair, int rank) {
      auto [index, value] = iv_pair;
      value += index + rank;
    };

    for (int i = 0; i < size; ++i) {
      arr.async_visit(i, square_visitor, world.rank());
    }

    int init_sum = (world.size() * (world.size() - 1)) / 2;

    arr.for_all([&world, &init_sum](auto iv_pair) {
      auto [index, value] = iv_pair;
      ASSERT_RELEASE(value == index * (world.size() + 1) + init_sum);
    });
  }

  return 0;
}
