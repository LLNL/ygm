// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#undef NDEBUG
#include <string>
#include <ygm/comm.hpp>
#include <ygm/container/map.hpp>

int main(int argc, char **argv) {
  ygm::comm world(&argc, &argv);

  //
  // Test Rank 0 async_insert
  {
    ygm::container::multimap<std::string, std::string> smap(world);
    if (world.rank() == 0) {
      smap.async_insert("dog", "cat");
      smap.async_insert("apple", "orange");
      smap.async_insert("red", "green");
    }
    YGM_ASSERT_RELEASE(smap.count("dog") == 1);
    YGM_ASSERT_RELEASE(smap.count("apple") == 1);
    YGM_ASSERT_RELEASE(smap.count("red") == 1);
  }

  //
  // Test all ranks async_insert
  {
    ygm::container::multimap<std::string, std::string> smap(world);

    smap.async_insert("dog", "cat");
    smap.async_insert("apple", "orange");
    smap.async_insert("red", "green");

    YGM_ASSERT_RELEASE(smap.count("dog") == (size_t)world.size());
    YGM_ASSERT_RELEASE(smap.count("apple") == (size_t)world.size());
    YGM_ASSERT_RELEASE(smap.count("red") == (size_t)world.size());
  }

  //
  // Test all ranks default & async_visit_if_contains
  {
    ygm::container::multimap<std::string, std::string> smap(world);
    smap.async_visit("dog",
                     [](const std::string &key, const std::string &value) {
                       YGM_ASSERT_RELEASE(key == "dog");
                       YGM_ASSERT_RELEASE(value == "");
                     });
    smap.async_visit("cat",
                     [](const std::string &key, const std::string &value) {
                       YGM_ASSERT_RELEASE(key == "cat");
                       YGM_ASSERT_RELEASE(value == "");
                     });
    smap.async_visit_if_contains("red", [](const auto &key, const auto &value) {
      YGM_ASSERT_RELEASE(false);
    });

    YGM_ASSERT_RELEASE(smap.count("dog") == 1);
    YGM_ASSERT_RELEASE(smap.count("cat") == 1);
    YGM_ASSERT_RELEASE(smap.count("red") == 0);

    YGM_ASSERT_RELEASE(smap.size() == 2);

    if (world.rank() == 0) {
      smap.async_erase("dog");
    }
    YGM_ASSERT_RELEASE(smap.count("dog") == 0);
    YGM_ASSERT_RELEASE(smap.size() == 1);
    smap.async_erase("cat");
    YGM_ASSERT_RELEASE(smap.count("cat") == 0);

    YGM_ASSERT_RELEASE(smap.size() == 0);
  }

  //
  // Test all ranks default & async_visit_if_contains (legacy)
  {
    ygm::container::multimap<std::string, std::string> smap(world);
    smap.async_visit("dog", [](const std::string &key, std::string &value) {
      YGM_ASSERT_RELEASE(key == "dog");
      YGM_ASSERT_RELEASE(value == "");
    });
    smap.async_visit("cat", [](const std::string &key, std::string &value) {
      YGM_ASSERT_RELEASE(key == "cat");
      YGM_ASSERT_RELEASE(value == "");
    });
    smap.async_visit_if_contains(
        "red", [](const auto &k, const auto &v) { YGM_ASSERT_RELEASE(false); });

    YGM_ASSERT_RELEASE(smap.count("dog") == 1);
    YGM_ASSERT_RELEASE(smap.count("cat") == 1);
    YGM_ASSERT_RELEASE(smap.count("red") == 0);

    YGM_ASSERT_RELEASE(smap.size() == 2);

    if (world.rank() == 0) {
      smap.async_erase("dog");
    }
    YGM_ASSERT_RELEASE(smap.count("dog") == 0);
    YGM_ASSERT_RELEASE(smap.size() == 1);
    smap.async_erase("cat");
    YGM_ASSERT_RELEASE(smap.count("cat") == 0);

    YGM_ASSERT_RELEASE(smap.size() == 0);
  }

  // //
  // // Test async_visit_group
  // {
  //   ygm::container::multimap<std::string, std::string> smap(world);

  //   // Insert from all ranks
  //   smap.async_insert("dog", "bark");
  //   smap.async_insert("dog", "woof");
  //   smap.async_insert("cat", "meow");

  //   world.barrier();

  //   smap.async_visit_group(
  //       "dog", [](auto pmap, const auto begin, const auto end) {
  //         YGM_ASSERT_RELEASE(std::distance(begin, end) == 2 *
  //         pmap->comm().size());
  //       });
  //   smap.async_visit_group(
  //       "cat", [](auto pmap, const auto begin, const auto end) {
  //         YGM_ASSERT_RELEASE(std::distance(begin, end) == pmap->comm().size());
  //       });
  // }

  //
  // Test swap & async_set
  {
    ygm::container::multimap<std::string, std::string> smap(world);
    {
      ygm::container::multimap<std::string, std::string> smap2(world);
      smap2.async_insert("dog", "cat");
      smap2.async_insert("apple", "orange");
      smap2.async_insert("red", "green");
      smap2.swap(smap);
      YGM_ASSERT_RELEASE(smap2.size() == 0);
    }
    YGM_ASSERT_RELEASE(smap.size() == 3 * (size_t)world.size());
    YGM_ASSERT_RELEASE(smap.count("dog") == (size_t)world.size());
    YGM_ASSERT_RELEASE(smap.count("apple") == (size_t)world.size());
    YGM_ASSERT_RELEASE(smap.count("red") == (size_t)world.size());
    smap.async_insert("car", "truck");
    YGM_ASSERT_RELEASE(smap.size() == 4 * (size_t)world.size());
    YGM_ASSERT_RELEASE(smap.count("car") == (size_t)world.size());
  }

  //
  // Test local_get()
  {
    ygm::container::multimap<std::string, std::string> smap(world);
    smap.async_insert("foo", "barr");
    smap.async_insert("foo", "baz");
    smap.async_insert("foo", "qux");
    smap.async_insert("foo", "quux");
    world.barrier();
    auto values = smap.local_get("foo");
    if (smap.partitioner.owner("foo") == world.rank()) {
      YGM_ASSERT_RELEASE(values.size() == 4 * (size_t)world.size());
    } else {
      YGM_ASSERT_RELEASE(values.size() == 0);
    }
  }

  //
  // Test for_all
  {
    ygm::container::multimap<std::string, std::string> smap1(world);
    ygm::container::multimap<std::string, std::string> smap2(world);

    smap1.async_insert("dog", "cat");
    smap1.async_insert("apple", "orange");
    smap1.async_insert("red", "green");

    smap1.for_all([&smap2](const auto &key, auto value) {
      smap2.async_insert(key, value);
    });

    YGM_ASSERT_RELEASE(smap2.count("dog") == (size_t)world.size());
    YGM_ASSERT_RELEASE(smap2.count("apple") == (size_t)world.size());
    YGM_ASSERT_RELEASE(smap2.count("red") == (size_t)world.size());
  }

  //
  // Test for_all (legacy lambdas)
  {
    ygm::container::multimap<std::string, std::string> smap1(world);
    ygm::container::multimap<std::string, std::string> smap2(world);

    smap1.async_insert("dog", "cat");
    smap1.async_insert("apple", "orange");
    smap1.async_insert("red", "green");

    smap1.for_all([&smap2](const auto &k, const auto &v) {
      smap2.async_insert(std::make_pair(k, v));
    });

    YGM_ASSERT_RELEASE(smap2.count("dog") == (size_t)world.size());
    YGM_ASSERT_RELEASE(smap2.count("apple") == (size_t)world.size());
    YGM_ASSERT_RELEASE(smap2.count("red") == (size_t)world.size());
  }

  return 0;
}
