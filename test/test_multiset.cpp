// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#undef NDEBUG
#include <string>

#include <ygm/comm.hpp>
#include <ygm/container/set.hpp>

int main(int argc, char** argv) {
  ygm::comm world(&argc, &argv);

  //
  // Test Rank 0 async_insert
  {
    ygm::container::multiset<std::string> sset(world);
    if (world.rank() == 0) {
      sset.async_insert("dog");
      sset.async_insert("apple");
      sset.async_insert("red");
    }
    ASSERT_RELEASE(sset.count("dog") == 1);
    ASSERT_RELEASE(sset.count("apple") == 1);
    ASSERT_RELEASE(sset.count("red") == 1);
    sset.async_erase("dog");
    ASSERT_RELEASE(sset.size() == 2);
    if (world.rank() == 0) { sset.async_erase("apple"); }
    ASSERT_RELEASE(sset.size() == 1);
    ASSERT_RELEASE(sset.count("dog") == 0);
    ASSERT_RELEASE(sset.count("apple") == 0);
  }

  //
  // Test all ranks async_insert
  {
    ygm::container::multiset<std::string> sset(world);

    sset.async_insert("dog");
    sset.async_insert("apple");
    sset.async_insert("red");

    ASSERT_RELEASE(sset.count("dog") == world.size());
    ASSERT_RELEASE(sset.count("apple") == world.size());
    ASSERT_RELEASE(sset.count("red") == world.size());
  }

  //
  // Test swap & async_set
  {
    ygm::container::multiset<std::string> sset(world);
    {
      ygm::container::multiset<std::string> sset2(world);
      sset2.async_insert("dog");
      sset2.async_insert("apple");
      sset2.async_insert("red");
      sset2.swap(sset);
      ASSERT_RELEASE(sset2.size() == 0);
    }
    ASSERT_RELEASE(sset.size() == 3 * world.size());
    ASSERT_RELEASE(sset.count("dog") == world.size());
    ASSERT_RELEASE(sset.count("apple") == world.size());
    ASSERT_RELEASE(sset.count("red") == world.size());
    sset.async_insert("car");
    ASSERT_RELEASE(sset.size() == 4 * world.size());
    ASSERT_RELEASE(sset.count("car") == world.size());
  }

  return 0;
}