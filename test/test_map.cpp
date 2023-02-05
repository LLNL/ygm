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
  // Test async_insert_if_missing
  {
    ygm::container::map<std::string, std::string> smap(world);

    smap.async_insert_if_missing("dog", "cat");
    smap.async_insert_if_missing("apple", "orange");

    world.barrier();

    smap.async_insert_if_missing("dog", "dog");
    smap.async_insert_if_missing("red", "green");

    world.barrier();

    smap.async_visit(
        "dog", [](auto key, auto &value) { ASSERT_RELEASE(value == "cat"); });
    smap.async_visit("apple", [](auto key, auto &value) {
      ASSERT_RELEASE(value == "orange");
    });
    smap.async_visit(
        "red", [](auto key, auto &value) { ASSERT_RELEASE(value == "green"); });
  }

  //
  // Test async_insert_if_missing (legacy lambdas)
  {
    ygm::container::map<std::string, std::string> smap(world);

    smap.async_insert_if_missing("dog", "cat");
    smap.async_insert_if_missing("apple", "orange");

    world.barrier();

    smap.async_insert_if_missing("dog", "dog");
    smap.async_insert_if_missing("red", "green");

    world.barrier();

    smap.async_visit("dog", [](std::pair<const std::string, std::string> &s) {
      ASSERT_RELEASE(s.second == "cat");
    });
    smap.async_visit("apple", [](std::pair<const std::string, std::string> &s) {
      ASSERT_RELEASE(s.second == "orange");
    });
    smap.async_visit("red", [](std::pair<const std::string, std::string> &s) {
      ASSERT_RELEASE(s.second == "green");
    });
  }

  //
  // Test all ranks default & async_visit_if_exists
  {
    ygm::container::map<std::string, std::string> smap(world, "default_string");
    smap.async_visit("dog", [](std::pair<const std::string, std::string> &s) {
      ASSERT_RELEASE(s.first == "dog");
      ASSERT_RELEASE(s.second == "default_string");
    });
    smap.async_visit("cat", [](std::pair<const std::string, std::string> &s) {
      ASSERT_RELEASE(s.first == "cat");
      ASSERT_RELEASE(s.second == "default_string");
    });
    smap.async_visit_if_exists("red",
                               [](const auto &p) { ASSERT_RELEASE(false); });

    ASSERT_RELEASE(smap.count("dog") == 1);
    ASSERT_RELEASE(smap.count("cat") == 1);
    ASSERT_RELEASE(smap.count("red") == 0);

    ASSERT_RELEASE(smap.size() == 2);

    if (world.rank() == 0) {
      smap.async_erase("dog");
    }
    ASSERT_RELEASE(smap.count("dog") == 0);
    ASSERT_RELEASE(smap.size() == 1);
    smap.async_erase("cat");
    ASSERT_RELEASE(smap.count("cat") == 0);

    ASSERT_RELEASE(smap.size() == 0);
  }

  //
  // Test async_insert_if_missing_else_visit
  {
    ygm::container::map<std::string, std::string> smap(world);

    smap.async_insert("dog", "cat");

    world.barrier();

    static int dog_visit_counter{0};

    smap.async_insert_if_missing_else_visit(
        "dog", "other_dog",
        [](const auto &kv, const auto &new_value) { dog_visit_counter++; });

    world.barrier();

    ASSERT_RELEASE(world.all_reduce_sum(dog_visit_counter) == world.size());

    static int apple_visit_counter{0};

    smap.async_insert_if_missing_else_visit(
        "apple", "orange",
        [](const auto &kv, const auto &new_value) { apple_visit_counter++; });

    world.barrier();

    ASSERT_RELEASE(world.all_reduce_sum(apple_visit_counter) ==
                   world.size() - 1);

    if (world.rank0()) {
      smap.async_insert_if_missing_else_visit(
          "red", "green", [](const auto &kv, const auto &new_value) {
            ASSERT_RELEASE(true == false);
          });
    }
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
    ygm::container::map<std::string, std::vector<std::string>> smap(world);
    auto str_push_back = [](std::pair<const auto, auto> &key_value,
                            const std::string           &str) {
      // auto str_push_back = [](auto key_value, const std::string &str) {
      key_value.second.push_back(str);
    };
    if (world.rank0()) {
      smap.async_visit("foo", str_push_back, std::string("bar"));
      smap.async_visit("foo", str_push_back, std::string("baz"));
    }

    std::vector<std::string> gather_list = {"foo"};

    if (!world.rank0()) {
      gather_list.clear();
    }

    auto gmap = smap.all_gather(gather_list);

    if (world.rank0()) {
      ASSERT_RELEASE(gmap["foo"][0] == "bar");
      ASSERT_RELEASE(gmap["foo"][1] == "baz");
    } else {
      ASSERT_RELEASE(gmap["foo"].empty());
    }
  }

  //
  // Test for_all
  {
    ygm::container::map<std::string, std::string> smap1(world);
    ygm::container::map<std::string, std::string> smap2(world);

    smap1.async_insert("dog", "cat");
    smap1.async_insert("apple", "orange");
    smap1.async_insert("red", "green");

    smap1.for_all([&smap2](const auto &key, const auto &value) {
      smap2.async_insert(key, value);
    });

    ASSERT_RELEASE(smap2.count("dog") == 1);
    ASSERT_RELEASE(smap2.count("apple") == 1);
    ASSERT_RELEASE(smap2.count("red") == 1);
  }

  //
  // Test for_all (legacy lambdas)
  {
    ygm::container::map<std::string, std::string> smap1(world);
    ygm::container::map<std::string, std::string> smap2(world);

    smap1.async_insert("dog", "cat");
    smap1.async_insert("apple", "orange");
    smap1.async_insert("red", "green");

    smap1.for_all([&smap2](const auto &kv) { smap2.async_insert(kv); });

    ASSERT_RELEASE(smap2.count("dog") == 1);
    ASSERT_RELEASE(smap2.count("apple") == 1);
    ASSERT_RELEASE(smap2.count("red") == 1);
  }

  return 0;
}
