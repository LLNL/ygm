// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#undef NDEBUG

#include <ygm/comm.hpp>
#include <ygm/container/array.hpp>

int main(int argc, char **argv) {
  ygm::comm world(&argc, &argv);

  // Test basic tagging
  {
    int                        size = 64;
    ygm::container::array<int> arr(world, size);

    static_assert(std::is_same_v<decltype(arr)::self_type, decltype(arr)>);
    static_assert(std::is_same_v<decltype(arr)::mapped_type, decltype(size)>);
    static_assert(std::is_same_v<decltype(arr)::key_type, size_t>);
    static_assert(
        std::is_same_v<decltype(arr)::size_type, decltype(arr)::key_type>);
    static_assert(
        std::is_same_v<
            decltype(arr)::for_all_args,
            std::tuple<decltype(arr)::key_type, decltype(arr)::mapped_type> >);
  }

  // Test async_set
  {
    int                        size = 64;
    ygm::container::array<int> arr(world, size);

    if (world.rank0()) {
      for (int i = 0; i < size; ++i) {
        arr.async_set(i, i);
      }
    }

    arr.for_all([](const auto index, const auto value) {
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

    arr.for_all([&world](const auto index, const auto value) {
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

    arr.for_all([&world](const auto index, const auto value) {
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

    arr.for_all([&world](const auto index, const auto value) {
      ASSERT_RELEASE(value == index + world.size());
    });
  }

  // Test async_visit
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
      arr.async_visit(i, [](const auto index, const auto value) {
        ASSERT_RELEASE(value == index);
      });
    }
  }

  // Test async_visit (ptr)
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
      arr.async_visit(i, [](auto ptr, const auto index, const auto value) {
        ASSERT_RELEASE(value == index);
      });
    }
  }

  // Test value-only for_all
  {
    int size = 64;

    ygm::container::array<int> arr(world, size);

    if (world.rank0()) {
      for (int i = 0; i < size; ++i) {
        arr.async_set(i, 1);
      }
    }

    world.barrier();

    for (int i = 0; i < size; ++i) {
      arr.async_increment(i);
    }

    arr.for_all([&world](const auto value) {
      ASSERT_RELEASE(value == world.size() + 1);
    });
  }

  // Test copy constructor
  {
    int size = 64;

    ygm::container::array<int> arr(world, size);

    if (world.rank0()) {
      for (int i = 0; i < size; ++i) {
        arr.async_set(i, 2 * i);
      }
    }

    world.barrier();

    ygm::container::array<int> arr_copy(arr);

    arr_copy.for_all([&arr](const auto &index, const auto &value) {
      arr.async_visit(
          index,
          [](const auto &index, const auto &my_value, const auto &other_value) {
            ASSERT_RELEASE(my_value == other_value);
          },
          value);
    });

    arr.for_all([&arr_copy](const auto &index, const auto &value) {
      arr_copy.async_visit(
          index,
          [](const auto &index, const auto &my_value, const auto &other_value) {
            ASSERT_RELEASE(my_value == other_value);
          },
          value);
    });
  }

  // Test resize
  {
    int large_size = 64;
    int small_size = 32;

    ygm::container::array<int> arr(world, large_size);

    if (world.rank0()) {
      for (int i = 0; i < large_size; ++i) {
        arr.async_set(i, 2 * i);
      }
    }

    world.barrier();

    ASSERT_RELEASE(arr.size() == large_size);
    arr.for_all([](const auto &index, const auto &value) {
      ASSERT_RELEASE(value == 2 * index);
    });

    arr.resize(small_size);

    ASSERT_RELEASE(arr.size() == small_size);
    arr.for_all([](const auto &index, const auto &value) {
      ASSERT_RELEASE(value == 2 * index);
    });

    arr.resize(large_size);

    ASSERT_RELEASE(arr.size() == large_size);
    arr.for_all([&small_size](const auto &index, const auto &value) {
      if (index < small_size) {
        ASSERT_RELEASE(value == 2 * index);
      }
    });
  }

  // Test clear
  {
    int initial_size = 64;

    ygm::container::array<int> arr(world, initial_size);

    if (world.rank0()) {
      for (int i = 0; i < initial_size; ++i) {
        arr.async_set(i, 2 * i);
      }
    }

    world.barrier();

    ASSERT_RELEASE(arr.size() == initial_size);

    arr.clear();

    ASSERT_RELEASE(arr.size() == 0);
  }

  // Test swap
  {
    int size1 = 32;
    int size2 = 48;

    ygm::container::array<int> arr1(world, size1);
    ygm::container::array<int> arr2(world, size2);

    if (world.rank0()) {
      for (int i = 0; i < size1; ++i) {
        arr1.async_set(i, 2 * i);
      }
      for (int i = 0; i < size2; ++i) {
        arr2.async_set(i, 3 * i + 1);
      }
    }

    world.barrier();

    ASSERT_RELEASE(arr1.size() == size1);
    ASSERT_RELEASE(arr2.size() == size2);

    arr1.for_all([](const auto &index, const auto &value) {
      ASSERT_RELEASE(value == 2 * index);
    });

    arr2.for_all([](const auto &index, const auto &value) {
      ASSERT_RELEASE(value == 3 * index + 1);
    });

    arr1.swap(arr2);

    ASSERT_RELEASE(arr1.size() == size2);
    ASSERT_RELEASE(arr2.size() == size1);

    arr1.for_all([](const auto &index, const auto &value) {
      ASSERT_RELEASE(value == 3 * index + 1);
    });

    arr2.for_all([](const auto &index, const auto &value) {
      ASSERT_RELEASE(value == 2 * index);
    });
  }

  return 0;
}
