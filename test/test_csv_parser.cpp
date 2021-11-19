// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#undef NDEBUG

#include <filesystem>
#include <ygm/comm.hpp>
#include <ygm/io/csv_parser.hpp>

int main(int argc, char** argv) {
  ygm::comm world(&argc, &argv);

  size_t              local_count{0};
  ygm::io::csv_parser csvp(world, std::vector<std::string>{"data/100.csv"});
  csvp.for_all([&world, &local_count](const auto& vfields) {
    for (auto f : vfields) {
      ASSERT_RELEASE(f.is_integer());
      local_count += f.as_integer();
    }
  });

  world.barrier();
  ASSERT_RELEASE(world.all_reduce_sum(local_count) == 100);

  return 0;
}
