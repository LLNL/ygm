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

  //
  // Test basic tagging
  {
    ygm::container::set<std::string> sset(world);

    static_assert(std::is_same_v<decltype(sset)::self_type, decltype(sset)>);
    static_assert(std::is_same_v<decltype(sset)::value_type, std::string>);
    static_assert(std::is_same_v<decltype(sset)::size_type, size_t>);
    static_assert(std::is_same_v<decltype(sset)::for_all_args,
                                 std::tuple<decltype(sset)::value_type>>);
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
    ASSERT_RELEASE(sset.count("red") == 1);
    ASSERT_RELEASE(sset.count("apple") == 1);
    ASSERT_RELEASE(sset.size() == 3);

    ygm::container::set<int> iset(world);
    if (world.rank() == 0) {
      iset.async_insert(42);
      iset.async_insert(7);
      iset.async_insert(100);
    }
    ASSERT_RELEASE(iset.count(42) == 1);
    ASSERT_RELEASE(iset.count(7) == 1);
    ASSERT_RELEASE(iset.count(100) == 1);
    ASSERT_RELEASE(iset.size() == 3);
  }

  //
  // Test Rank 0 async_insert with ygm set pointer
  {
    ygm::container::set<std::string> sset(world);
    auto sset_ptr = sset.get_ygm_ptr();
    if (world.rank() == 0) {
      sset_ptr->async_insert("dog");
      sset_ptr->async_insert("apple");
      sset_ptr->async_insert("red");
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
  // Test async_contains
  {
    static bool              set_contains = false;
    ygm::container::set<int> iset(world);
    int val = 42;

    auto f = [](bool& contains, const int& i) {
      set_contains = contains;
    };   

    if (world.rank0()) {
      iset.async_contains(val, f);
    }
    world.barrier();
    // bool g_set_contains = ygm::logical_or(set_contains, world);   
    ASSERT_RELEASE(not ygm::logical_or(set_contains, world));

    if (world.rank0()) {
      iset.async_insert(val);
    }
    world.barrier();

    if (world.rank0()) {
      iset.async_contains(val, f);
    }
    world.barrier();
    ASSERT_RELEASE(ygm::logical_or(set_contains, world));
  }

  //
  // Test async_insert_contains
  {
    static bool              did_contain = false;
    ygm::container::set<std::string> sset(world);

    auto f = [](bool& contains, const std::string& s) {
      did_contain = contains;
    };   

    if (world.rank0()) {
      sset.async_insert_contains("dog", f);
    }
    world.barrier();
    ASSERT_RELEASE(not ygm::logical_or(did_contain, world));

    if (world.rank0()) {
      sset.async_insert_contains("dog", f);
    }
    world.barrier();
    ASSERT_RELEASE(ygm::logical_or(did_contain, world));
  }


  //
  // Test additional arguments of async_contains
  // {
  //   ygm::container::set<std::string> sset(world);
  //   sset.async_contains("howdy", [](bool c, const std::string s, int i, float f){}, 3, 3.14);
  //   sset.async_contains("howdy", [](auto ptr_set, bool c, const std::string s){});
  //   world.barrier();
  // }


  //
  // Test swap
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

  // //
  // // Test consume_all
  // {
  //   ygm::container::set<std::string> sset1(world);
  //   ygm::container::set<std::string> sset2(world);

  //   sset1.async_insert("dog");
  //   sset1.async_insert("apple");
  //   sset1.async_insert("red");

  //   sset1.consume_all([&sset2](const auto &key) { sset2.async_insert(key); });

  //   ASSERT_RELEASE(sset1.empty());
  //   ASSERT_RELEASE(sset2.count("dog") == 1);
  //   ASSERT_RELEASE(sset2.count("apple") == 1);
  //   ASSERT_RELEASE(sset2.count("red") == 1);
  // }

  // //
  // // Test consume_all_iterative
  // {
  //   ygm::container::set<std::string> sset1(world);
  //   ygm::container::set<std::string> sset2(world);

  //   sset1.async_insert("dog");
  //   sset1.async_insert("apple");
  //   sset1.async_insert("red");

  //   ygm::consume_all_iterative_adapter cai(sset1);
  //   cai.consume_all([&sset2](const auto &key) { sset2.async_insert(key); });

  //   ASSERT_RELEASE(sset1.empty());
  //   ASSERT_RELEASE(sset2.count("dog") == 1);
  //   ASSERT_RELEASE(sset2.count("apple") == 1);
  //   ASSERT_RELEASE(sset2.count("red") == 1);
  // }

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
