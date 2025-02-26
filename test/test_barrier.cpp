// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#undef NDEBUG

#include <ygm/comm.hpp>

int main(int argc, char **argv) {
  ygm::comm world(&argc, &argv);

  // Test barriers for early exit
  {
    int        num_rounds = 100;
    static int round      = 0;
    for (int i = 0; i < num_rounds; ++i) {
      world.async_bcast(
          [](int curr_round) { YGM_ASSERT_RELEASE(curr_round == round); },
          round);

      world.barrier();

      ++round;
    }
  }

  return 0;
}
