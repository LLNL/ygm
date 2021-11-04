# Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
# Project Developers. See the top-level COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

#
# This function adds an mpi example.
#
function ( add_mpi_example example_name )
  set(example_source "${example_name}.cpp")
  set(example_exe    "${example_name}")
  add_executable(${example_exe} ${example_source})
  target_link_libraries(${example_exe} ygm)
endfunction()

add_mpi_example(howdy_world)
add_mpi_example(howdy_world_recursive)
add_mpi_example(bandwidth_check)
add_mpi_example(bag_insert_rate)
add_mpi_example(word_counter)
add_mpi_example(lambda_optional_arguments)
add_mpi_example(map_visit_optional_arguments)
add_mpi_example(map_set)
add_mpi_example(map_visit)
add_mpi_example(multimap_visit_group)
