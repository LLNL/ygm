// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#undef NDEBUG

#include <string>

#include <ygm/comm.hpp>
#include <ygm/container/set.hpp>

int main(int argc, char **argv) {
  ygm::comm world(&argc, &argv);

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
  // Test local_for_random_samples
  {
    int size           = world.size() * 8;
    int local_requests = 4;

    ygm::container::set<int> iset(world);

    if (world.rank0()) {
      for (int i(0); i < size; ++i) {
        iset.async_insert(i);
      }
    }

    world.barrier();

    static int local_fulfilled(0);
    iset.local_for_random_samples(
        local_requests, [&world](const auto &elem) { ++local_fulfilled; });

    world.barrier();

    ASSERT_RELEASE(local_requests == local_fulfilled);
  }

  return 0;
}