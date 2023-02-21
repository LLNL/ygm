// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#undef NDEBUG

#include <string>
#include <ygm/comm.hpp>
#include <ygm/container/bag.hpp>

int main(int argc, char** argv) {
  ygm::comm world(&argc, &argv);

  //
  // Test Rank 0 async_insert
  {
    ygm::container::bag<std::string> bbag(world);
    if (world.rank0()) {
      bbag.async_insert("dog");
      bbag.async_insert("apple");
      bbag.async_insert("red");
    }
    ASSERT_RELEASE(bbag.size() == 3);
  }

  //
  // Test all ranks async_insert
  {
    ygm::container::bag<std::string> bbag(world);
    bbag.async_insert("dog");
    bbag.async_insert("apple");
    bbag.async_insert("red");
    ASSERT_RELEASE(bbag.size() == 3 * (size_t)world.size());
    
    auto all_data = bbag.gather_to_vector(0);
    if(world.rank0()) {
      ASSERT_RELEASE(all_data.size() == 3 * (size_t)world.size());
    }
  }

  {
    ygm::container::bag<int> bbag(world);
    int num_of_items = 20;
    if (world.rank0()) {
      for (int i = 0; i < num_of_items; i++) {
        bbag.async_insert(i);
      }
    }
    std::default_random_engine rand_eng1 = std::default_random_engine(std::random_device()());
    bbag.local_shuffle(rand_eng1);

    std::default_random_engine rand_eng2 = std::default_random_engine(std::random_device()());
    bbag.global_shuffle(rand_eng2);

    ASSERT_RELEASE(bbag.size() == num_of_items);

    auto bag_content = bbag.gather_to_vector(0);
    if (world.rank0()) {
      bool all_items_present = true;
      for (int i = 0; i < num_of_items; i++) {
        if (std::find(bag_content.begin(), bag_content.end(), i) == bag_content.end()) {
          all_items_present = false;
        }
      }
      ASSERT_RELEASE(all_items_present);
    }
  }
}