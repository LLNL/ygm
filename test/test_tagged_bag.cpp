// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#undef NDEBUG

#include <initializer_list>
#include <string>
#include <vector>
#include <ygm/comm.hpp>
#include <ygm/container/tagged_bag.hpp>

using ygm::container::tag_type;
using ygm::container::tagged_bag;
int main(int argc, char** argv) {
  ygm::comm world(&argc, &argv);

  //
  // Test Rank 0 async_insert
  {
    tagged_bag<std::string> tagbag(world);
    tag_type                r0t1{};
    tag_type                r0t2{};
    tag_type                r0t3{};
    std::vector<tag_type>   r0tags{};
    if (world.rank0()) {
      r0t1   = tagbag.async_insert("dog");
      r0t2   = tagbag.async_insert("apple");
      r0t3   = tagbag.async_insert("red");
      r0tags = std::vector<tag_type>{r0t1, r0t2, r0t3};
    }

    ASSERT_RELEASE(tagbag.size() == 3);
    // Test gather
    auto gather = tagbag.all_gather(r0tags);
    world.barrier();
    if (world.rank0()) {
      ASSERT_RELEASE(gather.size() == 3);
    } else {
      ASSERT_RELEASE(gather.empty());
    }

    tagbag.for_all([](auto& k, auto& v) { v += "_added"; });

    auto gatheradd = tagbag.all_gather(r0tags);
    if (world.rank0()) {
      for (auto r0tag : r0tags) {
        auto ga = gatheradd.at(r0tag);
        ASSERT_RELEASE(ga.substr(ga.size() - 6) == "_added");
      }
    }
  }

  //
  // Test all ranks async_insert
  {
    ygm::container::tagged_bag<std::string> bbag(world);
    bbag.async_insert("dog");
    bbag.async_insert("apple");
    bbag.async_insert("red");
    ASSERT_RELEASE(bbag.size() == 3 * (size_t)world.size());
  }
}