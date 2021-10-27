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

add_mpi_example(counter_scaling_test)
