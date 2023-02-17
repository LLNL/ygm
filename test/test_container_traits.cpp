// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#undef NDEBUG

#include <ygm/comm.hpp>
#include <ygm/container/detail/container_traits.hpp>
#include <ygm/detail/ygm_traits.hpp>

int main(int argc, char** argv) {
  ygm::comm world(&argc, &argv);

  ygm::container::bag<int>          test_bag(world);
  ygm::container::set<int>          test_set(world);
  ygm::container::map<int, int>     test_map(world);
  ygm::container::array<int>        test_array(world, 10);
  ygm::container::counting_set<int> test_counting_set(world);
  ygm::container::disjoint_set<int> test_disjoint_set(world);
  int                               i;

  //
  // Test is_bag
  {
    static_assert(ygm::container::detail::is_bag<decltype(test_bag)>);
    static_assert(not ygm::container::detail::is_bag<decltype(test_set)>);
    static_assert(not ygm::container::detail::is_bag<decltype(test_map)>);
    static_assert(not ygm::container::detail::is_bag<decltype(test_array)>);
    static_assert(
        not ygm::container::detail::is_bag<decltype(test_counting_set)>);
    static_assert(
        not ygm::container::detail::is_bag<decltype(test_disjoint_set)>);
    static_assert(not ygm::container::detail::is_bag<decltype(i)>);
  }

  //
  // Test is_set
  {
    static_assert(not ygm::container::detail::is_set<decltype(test_bag)>);
    static_assert(ygm::container::detail::is_set<decltype(test_set)>);
    static_assert(not ygm::container::detail::is_set<decltype(test_map)>);
    static_assert(not ygm::container::detail::is_set<decltype(test_array)>);
    static_assert(
        not ygm::container::detail::is_set<decltype(test_counting_set)>);
    static_assert(
        not ygm::container::detail::is_set<decltype(test_disjoint_set)>);
    static_assert(not ygm::container::detail::is_set<decltype(i)>);
  }

  //
  // Test is_map
  {
    static_assert(not ygm::container::detail::is_map<decltype(test_bag)>);
    static_assert(not ygm::container::detail::is_map<decltype(test_set)>);
    static_assert(ygm::container::detail::is_map<decltype(test_map)>);
    static_assert(not ygm::container::detail::is_map<decltype(test_array)>);
    static_assert(
        not ygm::container::detail::is_map<decltype(test_counting_set)>);
    static_assert(
        not ygm::container::detail::is_map<decltype(test_disjoint_set)>);
    static_assert(not ygm::container::detail::is_map<decltype(i)>);
  }

  //
  // Test is_array
  {
    static_assert(not ygm::container::detail::is_array<decltype(test_bag)>);
    static_assert(not ygm::container::detail::is_array<decltype(test_set)>);
    static_assert(not ygm::container::detail::is_array<decltype(test_map)>);
    static_assert(ygm::container::detail::is_array<decltype(test_array)>);
    static_assert(
        not ygm::container::detail::is_array<decltype(test_counting_set)>);
    static_assert(
        not ygm::container::detail::is_array<decltype(test_disjoint_set)>);
    static_assert(not ygm::container::detail::is_array<decltype(i)>);
  }

  //
  // Test is_counting_set
  {
    static_assert(
        not ygm::container::detail::is_counting_set<decltype(test_bag)>);
    static_assert(
        not ygm::container::detail::is_counting_set<decltype(test_set)>);
    static_assert(
        not ygm::container::detail::is_counting_set<decltype(test_map)>);
    static_assert(
        not ygm::container::detail::is_counting_set<decltype(test_array)>);
    static_assert(
        ygm::container::detail::is_counting_set<decltype(test_counting_set)>);
    static_assert(not ygm::container::detail::is_counting_set<
                  decltype(test_disjoint_set)>);
    static_assert(not ygm::container::detail::is_counting_set<decltype(i)>);
  }

  //
  // Test is_disjoint_set
  {
    static_assert(
        not ygm::container::detail::is_disjoint_set<decltype(test_bag)>);
    static_assert(
        not ygm::container::detail::is_disjoint_set<decltype(test_set)>);
    static_assert(
        not ygm::container::detail::is_disjoint_set<decltype(test_map)>);
    static_assert(
        not ygm::container::detail::is_disjoint_set<decltype(test_array)>);
    static_assert(not ygm::container::detail::is_disjoint_set<
                  decltype(test_counting_set)>);
    static_assert(
        ygm::container::detail::is_disjoint_set<decltype(test_disjoint_set)>);
    static_assert(not ygm::container::detail::is_disjoint_set<decltype(i)>);
  }

  return 0;
}
