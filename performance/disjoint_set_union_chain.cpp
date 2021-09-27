// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

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

  world.cout0("Global unions: ", num_unions);

  size_t num_local_unions =
      num_unions / world.size() + (world.size() < num_unions % world.size());
  size_t local_offset =
      (num_unions / world.size()) * world.rank() +
      std::min<size_t>(world.rank(), num_unions % world.size());

  ygm::container::disjoint_set<size_t> dset(world);

  /*****************************
   * Time unions
   ****************************/
  world.barrier();

  ygm::timer union_timer{};

  // Perform unions
  for (size_t i = local_offset; i < local_offset + num_local_unions; ++i) {
    dset.async_union(i, i + 1);
  }

  world.barrier();

  world.cout0("Performed ", num_unions, " unions in ", union_timer.elapsed(),
              " seconds");

  /*****************************
   * Time finds
   ****************************/
  std::vector<size_t> to_find(num_local_unions);

  for (size_t i = 0; i < num_local_unions; ++i) {
    to_find[i] = local_offset + i;
  }

  world.barrier();

  ygm::timer find_timer{};

  dset.all_find(to_find);

  world.barrier();

  world.cout0("Performed find on all ", num_unions, " items in ",
              find_timer.elapsed(), " seconds");

  world.cout() << "Local bytes sent: " << world.local_bytes_sent() << std::endl;

  return 0;
}
