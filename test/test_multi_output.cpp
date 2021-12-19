// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#undef NDEBUG

#include <filesystem>
#include <ygm/comm.hpp>
#include <ygm/io/multi_output.hpp>

int main(int argc, char **argv) {
  ygm::comm world(&argc, &argv);

  ygm::io::multi_output mo(world, std::string{"test_dir/nested_dir/prefix"},
                           false);

  mo.async_write_line("out", world.rank(), " my message");

  return 0;
}
