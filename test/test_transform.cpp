// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#undef NDEBUG

#include <set>
#include <string>
#include <vector>
#include <ygm/comm.hpp>
#include <ygm/container/bag.hpp>
#include <ygm/container/counting_set.hpp>
#include <ygm/random.hpp>

int main(int argc, char** argv) {
  ygm::comm world(&argc, &argv);

  {
    ygm::container::bag<int> ibag(world, {42, 1, 8, 16, 32, 3, 4, 5, 6, 7});

    int sum = ibag.transform([](int i){ return i+1; }).reduce(std::plus<int>());
    YGM_ASSERT_RELEASE(sum = 134);
  }

  {
    ygm::container::map<std::string, size_t> mymap(world);
    if(world.rank0()) {
      mymap.async_insert("red", 0);
      mymap.async_insert("green", 1);
      mymap.async_insert("blue", 2);
    }

    size_t slength = mymap.keys().transform([](std::string s){return s.size();}).reduce(std::plus<int>());
    YGM_ASSERT_RELEASE(slength = 12);

    int vsum = mymap.values().reduce(std::plus<int>());
    YGM_ASSERT_RELEASE(vsum = 3);
  }

}