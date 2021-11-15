// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#include <cstdlib>
#include <iostream>
#include <random>
#include <ygm/comm.hpp>
#include <ygm/utility.hpp>

// This is just an example to check bandwidth when sending messages with a no-op
// to be done by the receiver. The #pragma omp's need to be uncommented for
// multithreaded versions and commented out for unthreaded.

int main(int argc, char** argv) {
  ygm::comm world(&argc, &argv);
  {
    int         num_nodes{atoi(std::getenv("SLURM_NNODES"))};
    int         num_tasks{atoi(std::getenv("SLURM_NTASKS"))};
    std::string cluster_name(std::getenv("SLURM_CLUSTER_NAME"));

    world.cout0("Bandwidth check on ", cluster_name, " with ", num_tasks,
                " tasks on ", num_nodes, " nodes.\n");

    int comm_rank = world.rank();
    int comm_size = world.size();

    {  // Send vectors
      world.cout0("Bandwidth sending vectors");
      int msgs_per_node{1024 * 1024};
      int msgs_per_rank = msgs_per_node * num_nodes / num_tasks;
      int msg_length{1024};

      std::vector<int64_t> to_send;
      for (size_t i = 0; i < msg_length; ++i) {
        to_send.push_back(i);
      }

      world.barrier();
      ygm::timer send_timer{};

      std::mt19937                       gen(4567 * comm_rank);
      std::uniform_int_distribution<int> dest_dist(0, comm_size - 1);

      for (int msg = 0; msg < msgs_per_rank; ++msg) {
        world.async(dest_dist(gen),
                    [](auto mbox, const std::vector<int64_t>& vec) { return; },
                    to_send);
      }

      world.barrier();
      double elapsed = send_timer.elapsed();
      // Each message corresponds to 8 bytes per int64_t and 8 bytes for the
      // function pointer
      double bandwidth = float(msgs_per_rank) * num_tasks *
                         (8 * msg_length + 8) / elapsed / (1024 * 1024 * 1024);
      world.cout0("Elapsed time: ", elapsed);
      world.cout0("Vector Bandwidth: ", bandwidth, " GB/s\n");
    }

    {  // Send individual int64_t's
      world.cout0("Bandwidth sending individual int64_t's");
      uint64_t msgs_per_node{1024 * 1024 * 1024};
      uint64_t msgs_per_rank = msgs_per_node * num_nodes / num_tasks;

      std::vector<int>                   destinations;
      std::mt19937                       gen(1234 * comm_rank);
      std::uniform_int_distribution<int> dest_dist(0, comm_size - 1);

      for (int msg = 0; msg < msgs_per_rank; ++msg) {
        destinations.push_back(dest_dist(gen));
      }

      world.barrier();
      ygm::timer send_timer{};

      for (uint64_t msg = 0; msg < msgs_per_rank; ++msg) {
        world.async(destinations[msg],
                    [](auto mbox, const int64_t val) { return; },
                    destinations[msg]);
      }

      world.barrier();
      double elapsed = send_timer.elapsed();
      // Each message corresponds to 8 bytes for the int64_t and 8 bytes for the
      // function pointer
      double bandwidth = float(msgs_per_rank) * num_tasks * (8 + 8) / elapsed /
                         (1024 * 1024 * 1024);
      world.cout0("Elapsed time: ", elapsed);
      world.cout0("int64_t Bandwidth: ", bandwidth, " GB/s\n");
    }
  }
}
