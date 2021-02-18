// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#include <cstdlib>
#include <iostream>
#include <omp.h>
#include <random>
#include <ygm/comm.hpp>
#include <ygm/container/bag.hpp>
#include <ygm/utility.hpp>

// This is just an example to check the insert rate for a distributed bag.
// The #pragma omp's need to be uncommented for
// multithreaded versions and commented out for unthreaded.

int main(int argc, char **argv) {
  ygm::comm world(&argc, &argv);
  {
    int num_nodes{atoi(std::getenv("SLURM_NNODES"))};
    int num_tasks{atoi(std::getenv("SLURM_NTASKS"))};
    std::string cluster_name(std::getenv("SLURM_CLUSTER_NAME"));

    world.cout0("Checking bag insert rate on ", cluster_name, " with ",
                num_tasks, " tasks on ", num_nodes, " nodes.\n");

    int comm_rank = world.rank();
    int comm_size = world.size();

    { // Insert vectors uint64_t's
      world.cout0("Insertion rate for vectors");

      uint64_t vec_length = 1024;
      uint64_t inserts_per_node{1024 * 1024};
      uint64_t inserts_per_rank = inserts_per_node * num_nodes / num_tasks;

      ygm::container::bag<std::vector<uint64_t>> my_bag(world);

      std::vector<uint64_t> to_send;
      for (int i = 0; i < vec_length; ++i) {
        to_send.push_back(i);
      }

      world.barrier();
      ygm::timer bag_timer{};

#pragma omp parallel
      {
#pragma omp for
        for (uint64_t i = 0; i < inserts_per_rank; ++i) {
          my_bag.async_insert(to_send);
        }
      }

      world.barrier();
      double elapsed = bag_timer.elapsed();
      double insert_rate =
          float(inserts_per_rank) * num_tasks / elapsed / (1000 * 1000 * 1000);
      // Each insert has 8 bytes for each uint64_t inserted along with 8 bytes
      // for a function pointer and 4 bytes for a pointer to the bag)
      double effective_bandwidth = float(inserts_per_rank) * num_tasks *
                                   (8 * vec_length + 8 + 4) / elapsed /
                                   (1024 * 1024 * 1024);
      world.cout0("Elapsed time: ", elapsed);
      world.cout0(
          "Insert rate: ", insert_rate,
          " billion inserts / second\n\tCorresponds to effective bandwidth: ",
          effective_bandwidth, " GB/s\n");
    }

    { // Insert individual uint64_t's
      world.cout0("Insertion rate for uint64_t's");

      uint64_t inserts_per_node{1024 * 1024 * 64};
      uint64_t inserts_per_rank = inserts_per_node * num_nodes / num_tasks;

      ygm::container::bag<uint64_t> my_bag(world);

      world.barrier();
      ygm::timer bag_timer{};

#pragma omp parallel
      {
#pragma omp for
        for (uint64_t i = 0; i < inserts_per_rank; ++i) {
          my_bag.async_insert(i);
        }
      }

      world.barrier();
      double elapsed = bag_timer.elapsed();
      double insert_rate =
          float(inserts_per_rank) * num_tasks / elapsed / (1000 * 1000 * 1000);
      // Each insert has a message of 20 bytes (8 for uint64_t inserted, 8 for
      // function pointer, 4 for pointer to bag)
      double effective_bandwidth = float(inserts_per_rank) * num_tasks *
                                   (8 + 8 + 4) / elapsed / (1024 * 1024 * 1024);
      world.cout0("Elapsed time: ", elapsed);
      world.cout0(
          "Insert rate: ", insert_rate,
          " billion inserts / second\n\tCorresponds to effective bandwidth: ",
          effective_bandwidth, " GB/s\n");
    }
  }
}
