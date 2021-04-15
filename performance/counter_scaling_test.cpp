// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#include <random>
#include <ygm/comm.hpp>
#include <ygm/utility.hpp>

int main(int argc, char **argv) {
  ygm::comm world(&argc, &argv);

  if (argc < 2) {
    world.cerr0("Please provide the number of messages to send");
    exit(EXIT_FAILURE);
  }

  size_t num_messages = atoll(argv[1]);
  size_t seed;

  if (argc == 3) {
    seed = atoll(argv[2]);
  } else {
    if (world.rank0()) {
      std::random_device rd;
      seed = rd();
    }
    MPI_Bcast(&seed, 1, MPI_UNSIGNED_LONG_LONG, 0, MPI_COMM_WORLD);
  }

  world.cout0("Global messages: ", num_messages);
  world.cout0("Seed: ", seed);

  // Generate all pairs of ranks sending and receiving
  std::mt19937 gen(seed);
  std::uniform_int_distribution<int> rank_dist(0, world.size() - 1);

  std::vector<int> to_send;
  size_t num_to_recv{0};

  for (size_t i = 0; i < num_messages; ++i) {
    auto src = rank_dist(gen);
    auto dest = rank_dist(gen);

    // Includes messages to self
    if (src == world.rank()) {
      to_send.push_back(dest);
    }
    if (dest == world.rank()) {
      ++num_to_recv;
    }
  }

  static size_t msgs_received{0};

  // Lambda to increment counter
  auto increment_lambda = []() { ++msgs_received; };

  ygm::timer send_timer{};

  for (const auto dest : to_send) {
    world.async(dest, increment_lambda);
  }

  world.barrier();

  double elapsed = send_timer.elapsed();
  world.cout0("Time: ", elapsed);
  world.cout0("Messages per second: ", num_messages / elapsed);

  // Make sure all messages were delivered
  ASSERT_RELEASE(msgs_received == num_to_recv);

  return 0;
}
