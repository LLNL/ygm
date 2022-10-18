// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#include <string>
#include <ygm/comm.hpp>

int main(int argc, char** argv) {
  // Construct a communicator that sends messages when it has 32MB of used send
  // buffer space
  ygm::comm world(&argc, &argv);

  // Define a "hello world" lambda to execute on a remote rank
  auto hello_world_lambda = [](const std::string& name) {
    std::cout << "Hello " << name << std::endl;
  };

  // Rank 0 tells rank 1 to greet the world
  if (world.rank0()) {
    world.async(1, hello_world_lambda, std::string("world"));
  }

  return 0;
}
