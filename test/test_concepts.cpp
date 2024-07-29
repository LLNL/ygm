// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#include <ygm/container/array.hpp>
#include <ygm/container/bag.hpp>
#include <ygm/container/detail/base_concepts.hpp>
#include <ygm/container/map.hpp>
#include <ygm/container/set.hpp>

#include <vector>

int main(int argc, char **argv) {
  using namespace ygm::container::detail;

  // Test SingleItemTuple
  {
    static_assert(SingleItemTuple<std::tuple<int>>);
    static_assert(not SingleItemTuple<std::tuple<int, int>>);
    static_assert(not SingleItemTuple<int>);
  }

  // Test DoubleItemTuple
  {
    static_assert(DoubleItemTuple<std::tuple<int, int>>);
    static_assert(DoubleItemTuple<std::pair<int, float>>);
    static_assert(not DoubleItemTuple<std::tuple<int>>);
    static_assert(not DoubleItemTuple<std::tuple<int, int, int>>);
    static_assert(not DoubleItemTuple<int>);
  }

  // Test AtLeastOneItemTuple
  {
    static_assert(AtLeastOneItemTuple<std::tuple<int, int>>);
    static_assert(AtLeastOneItemTuple<std::pair<int, float>>);
    static_assert(AtLeastOneItemTuple<std::tuple<int>>);
    static_assert(AtLeastOneItemTuple<std::tuple<int, int, int>>);
    static_assert(not AtLeastOneItemTuple<int>);
  }

  // Test HasForAll
  {
    static_assert(HasForAll<ygm::container::bag<int>>);
    static_assert(HasForAll<ygm::container::set<int>>);
    static_assert(HasForAll<ygm::container::map<int, float>>);
    static_assert(HasForAll<ygm::container::array<float>>);
    static_assert(not HasForAll<int>);
    static_assert(not HasForAll<std::vector<int>>);
  }

  /*
  // Test IsSame
  {
    static_assert(IsSame<int, int>);
    static_assert(IsSame<int, int &>);
    static_assert(IsSame<int, const int &>);
    static_assert(not IsSame<int, int *>);
    static_assert(not IsSame<int, float>);
  }
  */

  /*
  // Test IsInvocable
  {
    auto lambda1 = [](int) {};
    auto lambda2 = [](int, float) {};

    static_assert(IsInvocable<decltype(lambda1), int>);
    static_assert(not IsInvocable<decltype(lambda1), int, float>);
    static_assert(not IsInvocable<decltype(lambda2), int>);
    static_assert(IsInvocable<decltype(lambda2), int, float>);
  }
  */

  return 0;
}
