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

  // Test basic tagging 
  {
    ygm::container::counting_set<std::string> cset(world);

    static_assert(std::is_same_v< decltype(cset)::self_type,     decltype(cset) >);
    static_assert(std::is_same_v< decltype(cset)::mapped_type,   size_t >);
    static_assert(std::is_same_v< decltype(cset)::key_type,      std::string >);
    static_assert(std::is_same_v< decltype(cset)::size_type,     size_t >);
    static_assert(std::is_same_v< decltype(cset)::ygm_for_all_types,   
            std::tuple< decltype(cset)::key_type, size_t > >);
  }

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

  //
  // Test topk
  {
    ygm::container::counting_set<std::string> cset(world);

    cset.async_insert("dog");
    cset.async_insert("dog");
    cset.async_insert("dog");
    cset.async_insert("cat");
    cset.async_insert("cat");
    cset.async_insert("bird");

    auto topk = cset.topk(
        2, [](const auto &a, const auto &b) { return a.second > b.second; });

    ASSERT_RELEASE(topk[0].first == "dog");
    ASSERT_RELEASE(topk[0].second == 3 * world.size());
    ASSERT_RELEASE(topk[1].first == "cat");
    ASSERT_RELEASE(topk[1].second == 2 * world.size());
  }

  //
  // Test for_all
  {
    ygm::container::counting_set<std::string> cset1(world);
    ygm::container::counting_set<std::string> cset2(world);

    cset1.async_insert("dog");
    cset1.async_insert("dog");
    cset1.async_insert("dog");
    cset1.async_insert("cat");
    cset1.async_insert("cat");
    cset1.async_insert("bird");

    ASSERT_RELEASE(cset1.count("dog")  == (size_t)world.size() * 3);
    ASSERT_RELEASE(cset1.count("cat")  == (size_t)world.size() * 2);
    ASSERT_RELEASE(cset1.count("bird") == (size_t)world.size());
    ASSERT_RELEASE(cset1.count("red")  == 0);
    ASSERT_RELEASE(cset1.size() == 3);
    

    cset1.for_all([&cset2](const auto &key, const auto &value) {
      for (int i = 0; i < value; i++) {
        cset2.async_insert(key);
      }
    });

    ASSERT_RELEASE(cset2.count("dog")  == (size_t)world.size() * 3);
    ASSERT_RELEASE(cset2.count("cat")  == (size_t)world.size() * 2);
    ASSERT_RELEASE(cset2.count("bird") == (size_t)world.size());
    ASSERT_RELEASE(cset2.count("red")  == 0);
    ASSERT_RELEASE(cset2.size() == 3);


  }

  return 0;
}
