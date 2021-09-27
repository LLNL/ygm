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
  include_link_mpi(${example_exe})
  if(UNIX AND NOT APPLE)
    target_link_libraries(${example_exe} rt)
  endif()
  include_directories(${CMAKE_CURRENT_SOURCE_DIR})
endfunction()

find_package(OpenMP)




#
# This function adds an MPI + OpenMP example.
#
function ( add_mpi_omp_example example_name )
  set(example_source "${example_name}.cpp")
  set(example_exe    "${example_name}")
  add_executable(${example_exe} ${example_source})
  include_link_mpi(${example_exe})
  target_link_libraries(${example_exe}  OpenMP::OpenMP_CXX)
  if(UNIX AND NOT APPLE)
    target_link_libraries(${example_exe} rt)
  endif()
  include_directories(${CMAKE_CURRENT_SOURCE_DIR})
endfunction()

add_mpi_omp_example(counter_scaling_test)
add_mpi_omp_example(disjoint_set_union_chain)
