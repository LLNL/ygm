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
      num_unions / world.size() + (world.rank() < num_unions % world.size());
  size_t local_offset =
      (num_unions / world.size()) * world.rank() +
      std::min<size_t>(world.rank(), num_unions % world.size());

  /*****************************
   * Experiment with unions incrementing
   ****************************/
  {
    world.cout0("**** Forward unions ****");
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
     * Time all_compress
     ****************************/
    world.barrier();

    ygm::timer compress_timer{};

    dset.all_compress();

    world.barrier();

    world.cout0("Performed all_compress on ", num_unions, " items in ",
                compress_timer.elapsed(), " seconds");
  }

  /*****************************
   * Experiment with unions decrementing
   ****************************/
  {
    world.cout0("\n**** Backward unions ****");
    ygm::container::disjoint_set<size_t> dset(world);

    /*****************************
     * Time unions
     ****************************/
    world.barrier();

    ygm::timer union_timer{};

    // Perform unions
    for (size_t i = local_offset + num_local_unions; i > local_offset; --i) {
      dset.async_union(i - 1, i);
    }

    world.barrier();

    world.cout0("Performed ", num_unions, " unions in ", union_timer.elapsed(),
                " seconds");

    /*****************************
     * Time all_compress
     ****************************/
    world.barrier();

    ygm::timer compress_timer{};

    dset.all_compress();

    world.barrier();

    world.cout0("Performed all_compress on ", num_unions, " items in ",
                compress_timer.elapsed(), " seconds");
  }

  return 0;
}
