# Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
# Project Developers. See the top-level COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

#
# This function adds an mpi example.
#
function ( add_ygm_example example_name )
  set(example_source "${example_name}.cpp")
  set(example_exe    "${example_name}")
  add_executable(${example_exe} ${example_source})
  target_link_libraries(${example_exe} ygm)
endfunction()

add_ygm_example(howdy_world)
add_ygm_example(howdy_world_recursive)
add_ygm_example(bandwidth_check)
add_ygm_example(bag_insert_rate)
add_ygm_example(word_counter)
add_ygm_example(lambda_optional_arguments)
add_ygm_example(map_visit_optional_arguments)
add_ygm_example(map_set)
add_ygm_example(map_visit)
add_ygm_example(map_insert_if_missing)
add_ygm_example(multimap_visit_group)
add_ygm_example(disjoint_set_cc)
add_ygm_example(disjoint_set_spanning_tree)
add_ygm_example(disjoint_set_async_union_and_execute)
