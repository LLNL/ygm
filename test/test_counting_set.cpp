// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#undef NDEBUG
#include <string>

#include <ygm/comm.hpp>
#include <ygm/container/counting_set.hpp>

int main(int argc, char **argv) {
  ygm::comm world(&argc, &argv);

  //
  // Test Rank 0 async_insert
  {
    ygm::container::counting_set<std::string> cset(world);
    if (world.rank() == 0) {
      cset.async_insert("dog");
      cset.async_insert("apple");
      cset.async_insert("red");
    }

    ASSERT_RELEASE(cset.count("dog") == 1);
    ASSERT_RELEASE(cset.count("apple") == 1);
    ASSERT_RELEASE(cset.count("red") == 1);
    ASSERT_RELEASE(cset.size() == 3);

    auto count_map = cset.all_gather({"dog", "cat", "apple"});
    ASSERT_RELEASE(count_map["dog"] == 1);
    ASSERT_RELEASE(count_map["apple"] == 1);
    ASSERT_RELEASE(count_map.count("cat") == 0);
  }

  //
  // Test all ranks async_insert
  {
    ygm::container::counting_set<std::string> cset(world);

    cset.async_insert("dog");
    cset.async_insert("apple");
    cset.async_insert("red");

    ASSERT_RELEASE(cset.count("dog") == (size_t)world.size());
    ASSERT_RELEASE(cset.count("apple") == (size_t)world.size());
    ASSERT_RELEASE(cset.count("red") == (size_t)world.size());
    ASSERT_RELEASE(cset.size() == 3);

    auto count_map = cset.all_gather({"dog", "cat", "apple"});
    ASSERT_RELEASE(count_map["dog"] == (size_t)world.size());
    ASSERT_RELEASE(count_map["apple"] == (size_t)world.size());
    ASSERT_RELEASE(cset.count("cat") == 0);

    ASSERT_RELEASE(cset.count_all() == 3 * (size_t)world.size());
  }

  //
  // Test counting_sets YGM pointer
  {
    ygm::container::counting_set<std::string> cset(world);

    auto cset_ptr = cset.get_ygm_ptr();

    // Mix operations with pointer and counting_set
    cset_ptr->async_insert("dog");
    cset_ptr->async_insert("apple");
    cset.async_insert("red");

    ASSERT_RELEASE(cset_ptr->count("dog") == (size_t)world.size());
    ASSERT_RELEASE(cset_ptr->count("apple") == (size_t)world.size());
    ASSERT_RELEASE(cset.count("red") == (size_t)world.size());
    ASSERT_RELEASE(cset.size() == 3);

    auto count_map = cset.all_gather({"dog", "cat", "apple"});
    ASSERT_RELEASE(count_map["dog"] == (size_t)world.size());
    ASSERT_RELEASE(count_map["apple"] == (size_t)world.size());
    ASSERT_RELEASE(cset.count("cat") == 0);

    ASSERT_RELEASE(cset.count_all() == 3 * (size_t)world.size());
  }

  return 0;
}
