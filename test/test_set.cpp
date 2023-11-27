// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#include "ygm/detail/assert.hpp"
#undef NDEBUG

#include <string>

#include <ygm/comm.hpp>
#include <ygm/container/set.hpp>
#include <ygm/for_all_adapter.hpp>

int main(int argc, char **argv) {
  ygm::comm world(&argc, &argv);

  // Test basic tagging
  {
    ygm::container::set<std::string> sset(world);

    static_assert(std::is_same_v<decltype(sset)::self_type, decltype(sset)>);
    static_assert(std::is_same_v<decltype(sset)::key_type, std::string>);
    static_assert(std::is_same_v<decltype(sset)::size_type, size_t>);
    static_assert(std::is_same_v<decltype(sset)::ygm_for_all_types,
                                 std::tuple<decltype(sset)::key_type>>);
  }

  //
  // Test Rank 0 async_insert
  {
    ygm::container::set<std::string> sset(world);
    if (world.rank() == 0) {
      sset.async_insert("dog");
      sset.async_insert("apple");
      sset.async_insert("red");
    }
    ASSERT_RELEASE(sset.count("dog") == 1);
    ASSERT_RELEASE(sset.count("apple") == 1);
    ASSERT_RELEASE(sset.count("red") == 1);
    ASSERT_RELEASE(sset.size() == 3);
  }

    //
  // Test Rank 0 async_insert with ygm set pointer
  {
    ygm::container::set<std::string> sset(world);
    auto sset_ptr = sset.get_ygm_ptr();
    if (world.rank() == 0) {
      sset_ptr->async_insert_unique("dog");
      sset_ptr->async_insert_unique("apple");
      sset_ptr->async_insert_unique("red");
    }
    ASSERT_RELEASE(sset.count("dog") == 1);
    ASSERT_RELEASE(sset.count("apple") == 1);
    ASSERT_RELEASE(sset.count("red") == 1);
    ASSERT_RELEASE(sset.size() == 3);
  }

  

  //
  // Test all ranks async_insert
  {
    ygm::container::set<std::string> sset(world);

    sset.async_insert("dog");
    sset.async_insert("apple");
    sset.async_insert("red");

    ASSERT_RELEASE(sset.count("dog") == 1);
    ASSERT_RELEASE(sset.count("apple") == 1);
    ASSERT_RELEASE(sset.count("red") == 1);
    ASSERT_RELEASE(sset.size() == 3);
    sset.async_erase("dog");
    ASSERT_RELEASE(sset.count("dog") == 0);
    ASSERT_RELEASE(sset.size() == 2);
  }

  //
  // Test async_insert_exe_if_contains
  {
    static bool              found = false;
    ygm::container::set<int> iset(world);
    iset.async_insert_exe_if_contains(world.rank(), [](int) { found = true; });
    world.barrier();
    ASSERT_RELEASE(not found);

    iset.async_insert_exe_if_contains(world.rank(), [](int) { found = true; });
    world.barrier();
    ASSERT_RELEASE(found);
  }

  //
  // Test async_insert_exe_if_missing
  {
    static bool              missing = false;
    ygm::container::set<int> iset(world);
    iset.async_insert_exe_if_missing(world.rank(), [](int) { missing = true; });
    world.barrier();
    ASSERT_RELEASE(missing);
  }

  //
  // Test async_exe_if_missing
  {
    static bool              missing = false;
    ygm::container::set<int> iset(world);
    iset.async_exe_if_missing(world.rank(), [](int) { missing = true; });
    world.barrier();
    ASSERT_RELEASE(missing);

    iset.async_insert(world.rank());
    world.barrier();

    iset.async_exe_if_missing(world.rank(), [](int) { ASSERT_RELEASE(false); });
    world.barrier();
  }

  //
  // Test async_exe_if_contains
  {
    static bool              found = false;
    ygm::container::set<int> iset(world);
    iset.async_exe_if_contains(world.rank(), [](int) { found = true; });
    world.barrier();
    ASSERT_RELEASE(not found);

    iset.async_insert(world.rank());
    world.barrier();

    iset.async_exe_if_contains(world.rank(), [](int) { found = true; });
    world.barrier();
    ASSERT_RELEASE(found);
  }

  //
  // Test swap & async_set
  {
    ygm::container::set<std::string> sset(world);
    {
      ygm::container::set<std::string> sset2(world);
      sset2.async_insert("dog");
      sset2.async_insert("apple");
      sset2.async_insert("red");
      sset2.swap(sset);
      ASSERT_RELEASE(sset2.size() == 0);
    }
    ASSERT_RELEASE(sset.size() == 3);
    ASSERT_RELEASE(sset.count("dog") == 1);
    ASSERT_RELEASE(sset.count("apple") == 1);
    ASSERT_RELEASE(sset.count("red") == 1);
    sset.async_insert("car");
    ASSERT_RELEASE(sset.size() == 4);
    ASSERT_RELEASE(sset.count("car") == 1);
  }

  //
  // Test for_all
  {
    ygm::container::set<std::string> sset1(world);
    ygm::container::set<std::string> sset2(world);

    sset1.async_insert("dog");
    sset1.async_insert("apple");
    sset1.async_insert("red");

    sset1.for_all([&sset2](const auto &key) { sset2.async_insert(key); });

    ASSERT_RELEASE(sset2.count("dog") == 1);
    ASSERT_RELEASE(sset2.count("apple") == 1);
    ASSERT_RELEASE(sset2.count("red") == 1);
  }

  //
  // Test consume_all
  {
    ygm::container::set<std::string> sset1(world);
    ygm::container::set<std::string> sset2(world);

    sset1.async_insert("dog");
    sset1.async_insert("apple");
    sset1.async_insert("red");

    sset1.consume_all([&sset2](const auto &key) { sset2.async_insert(key); });

    ASSERT_RELEASE(sset1.empty());
    ASSERT_RELEASE(sset2.count("dog") == 1);
    ASSERT_RELEASE(sset2.count("apple") == 1);
    ASSERT_RELEASE(sset2.count("red") == 1);
  }

  //
  // Test consume_all_iterative
  {
    ygm::container::set<std::string> sset1(world);
    ygm::container::set<std::string> sset2(world);

    sset1.async_insert("dog");
    sset1.async_insert("apple");
    sset1.async_insert("red");

    ygm::consume_all_iterative_adapter cai(sset1);
    cai.consume_all([&sset2](const auto &key) { sset2.async_insert(key); });

    ASSERT_RELEASE(sset1.empty());
    ASSERT_RELEASE(sset2.count("dog") == 1);
    ASSERT_RELEASE(sset2.count("apple") == 1);
    ASSERT_RELEASE(sset2.count("red") == 1);
  }

  //
  // Test vector of sets
  {
    int                                   num_sets = 4;
    std::vector<ygm::container::set<int>> vec_sets;

    for (int i = 0; i < num_sets; ++i) {
      vec_sets.emplace_back(world);
    }

    for (int set_index = 0; set_index < num_sets; ++set_index) {
      int item = world.rank() + set_index;
      vec_sets[set_index].async_insert(item);
      vec_sets[set_index].async_insert(item + 1);
    }

    world.barrier();
    for (int set_index = 0; set_index < num_sets; ++set_index) {
      ASSERT_RELEASE(vec_sets[set_index].size() == world.size() + 1);
    }
  }

  return 0;
}
