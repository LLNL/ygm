// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#undef NDEBUG
#include <string>
#include <ygm/comm.hpp>
#include <ygm/container/map.hpp>

int main(int argc, char** argv) {
  ygm::comm world(&argc, &argv);

  //
  // Test Rank 0 async_insert
  {
    ygm::container::map<std::string, std::string> smap(world);
    if (world.rank() == 0) {
      smap.async_insert("dog", "cat");
      smap.async_insert("apple", "orange");
      smap.async_insert("red", "green");
    }
    ASSERT_RELEASE(smap.count("dog") == 1);
    ASSERT_RELEASE(smap.count("apple") == 1);
    ASSERT_RELEASE(smap.count("red") == 1);
  }

  //
  // Test all ranks async_insert
  {
    ygm::container::map<std::string, std::string> smap(world);

    smap.async_insert("dog", "cat");
    smap.async_insert("apple", "orange");
    smap.async_insert("red", "green");

    ASSERT_RELEASE(smap.count("dog") == 1);
    ASSERT_RELEASE(smap.count("apple") == 1);
    ASSERT_RELEASE(smap.count("red") == 1);
  }

  //
  // Test all ranks default & async_visit_if_exists
  {
    ygm::container::map<std::string, std::string> smap(world, "default_string");
    smap.async_visit("dog", [](const std::string& s1, const std::string& s2) {
      ASSERT_RELEASE(s1 == "dog");
      ASSERT_RELEASE(s2 == "default_string");
    });
    smap.async_visit("cat", [](const std::string& s1, const std::string& s2) {
      ASSERT_RELEASE(s1 == "cat");
      ASSERT_RELEASE(s2 == "default_string");
    });
    smap.async_visit_if_exists("red",
                               [](const auto& p) { ASSERT_RELEASE(false); });

    ASSERT_RELEASE(smap.count("dog") == 1);
    ASSERT_RELEASE(smap.count("cat") == 1);
    ASSERT_RELEASE(smap.count("red") == 0);

    ASSERT_RELEASE(smap.size() == 2);

    if (world.rank() == 0) { smap.async_erase("dog"); }
    ASSERT_RELEASE(smap.count("dog") == 0);
    ASSERT_RELEASE(smap.size() == 1);
    smap.async_erase("cat");
    ASSERT_RELEASE(smap.count("cat") == 0);

    ASSERT_RELEASE(smap.size() == 0);
  }

  //
  // Test swap & async_set
  {
    ygm::container::map<std::string, std::string> smap(world);
    {
      ygm::container::map<std::string, std::string> smap2(world);
      smap2.async_insert("dog", "cat");
      smap2.async_insert("apple", "orange");
      smap2.async_insert("red", "green");
      smap2.swap(smap);
      ASSERT_RELEASE(smap2.size() == 0);
    }
    ASSERT_RELEASE(smap.size() == 3);
    ASSERT_RELEASE(smap.count("dog") == 1);
    ASSERT_RELEASE(smap.count("apple") == 1);
    ASSERT_RELEASE(smap.count("red") == 1);
    smap.async_set("car", "truck");
    ASSERT_RELEASE(smap.size() == 4);
    ASSERT_RELEASE(smap.count("car") == 1);
  }

  //
  // Test map<vector>
  {
    ygm::container::map<std::string, std::vector<std::string> > smap(world);
    auto str_push_back = [](const auto& key, auto& value,
                            const std::string& str) { value.push_back(str); };
    if (world.rank0()) {
      smap.async_visit("foo", str_push_back, std::string("bar"));
      smap.async_visit("foo", str_push_back, std::string("baz"));
    }

    std::vector<std::string> gather_list = {"foo"};

    if (!world.rank0()) { gather_list.clear(); }

    auto gmap = smap.all_gather(gather_list);

    if (world.rank0()) {
      ASSERT_RELEASE(gmap["foo"][0] == "bar");
      ASSERT_RELEASE(gmap["foo"][1] == "baz");
    } else {
      ASSERT_RELEASE(gmap["foo"].empty());
    }
  }

  return 0;
}