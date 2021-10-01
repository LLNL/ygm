// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#include <random>
#include <ygm/comm.hpp>
#include <ygm/container/disjoint_set.hpp>
#include <ygm/utility.hpp>

int main(int argc, char **argv) {
  ygm::comm world(&argc, &argv);

  if (argc < 2) {
    world.cerr0("Please provide the number of unions to perform");
    exit(EXIT_FAILURE);
  }

  size_t num_unions = atoll(argv[1]);
  int    num_trials = 5;

  if (argc > 2) {
    num_trials = atoi(argv[2]);
  }

  world.cout0("Global unions: ", num_unions);
  world.cout0("Performing unions in random order");

  size_t num_local_unions =
      num_unions / world.size() + (world.rank() < num_unions % world.size());
  size_t local_offset =
      (num_unions / world.size()) * world.rank() +
      std::min<size_t>(world.rank(), num_unions % world.size());

  std::vector<size_t> my_unions;

  for (size_t i = local_offset; i < local_offset + num_local_unions; ++i) {
    my_unions.push_back(i);
  }

  std::random_device rd;
  std::mt19937       g(rd());

  double cumulative_union_time{0.0};
  double cumulative_compress_time{0.0};

  for (int trial = 0; trial < num_trials; ++trial) {
    world.cout0("\n********** Trial ", trial + 1, " **********");
    ygm::container::disjoint_set<size_t> dset(world);

    std::shuffle(my_unions.begin(), my_unions.end(), g);

    world.barrier();

    ygm::timer union_timer{};

    for (const auto &low_value : my_unions) {
      dset.async_union(low_value, low_value + 1);
    }

    world.barrier();

    double union_time = union_timer.elapsed();
    world.cout0("Union time: ", union_time);
    cumulative_union_time += union_time;

    world.barrier();

    ygm::timer compress_timer{};

    dset.all_compress();

    world.barrier();

    double compress_time = compress_timer.elapsed();
    world.cout0("Compress time: ", compress_time);
    cumulative_compress_time += compress_time;
  }

  world.cout0("\n********** Summary **********");
  world.cout0("Average union time: ", cumulative_union_time / num_trials);
  world.cout0("Average compress time: ", cumulative_compress_time / num_trials);

  return 0;
}
