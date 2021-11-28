// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#include <ygm/comm.hpp>
#include <ygm/container/map.hpp>

int main(int argc, char **argv) {
  ygm::comm world(&argc, &argv);

  ygm::container::map<std::string, std::string> my_map(world);

  // Inserts values for keys not-yet-seen.
  my_map.async_insert_if_missing("dog", "bark");
  my_map.async_insert_if_missing("cat", "meow");

  world.barrier();

  // No effect. "dog" already inserted.
  my_map.async_insert_if_missing("dog", "woof");

  world.barrier();

  auto sounds_lambda = [](auto &kv_pair, const auto &new_value,
                          const int origin_rank) {
    std::cout << "The " << kv_pair.first << " says " << kv_pair.second
              << " for rank " << origin_rank << std::endl;
  };

  // Keys already exist. Visits occur instead.
  my_map.async_insert_if_missing_else_visit("dog", "bow-wow", sounds_lambda,
                                            world.rank());
  my_map.async_insert_if_missing_else_visit("cat", "purr", sounds_lambda,
                                            world.rank());

  // First message to arrive causes an insert. All others perform a visit
  my_map.async_insert_if_missing_else_visit("bird", "chirp", sounds_lambda,
                                            world.rank());

  return 0;
}
