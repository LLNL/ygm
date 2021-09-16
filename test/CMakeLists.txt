# Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
# Project Developers. See the top-level COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT


#
# This function adds a Sequential test.
#
function ( add_seq_test test_name )
  set(test_source "${test_name}.cpp")
  set(test_exe    "SEQ_${test_name}")
  add_executable(${test_exe} ${test_source})
  if(UNIX AND NOT APPLE)
    target_link_libraries(${test_exe} rt)
  endif()
  include_directories(${CMAKE_CURRENT_SOURCE_DIR})
  add_test( ${test_exe} "${CMAKE_CURRENT_BINARY_DIR}/${test_exe}")
endfunction()

#
# This function adds an MPI test.
#
function ( add_mpi_test test_name )
  set(test_source "${test_name}.cpp")
  set(test_exe    "MPI_${test_name}")
  add_executable(${test_exe} ${test_source})
  include_link_mpi(${test_exe})
  if(UNIX AND NOT APPLE)
    target_link_libraries(${test_exe} rt)
  endif()
  include_directories(${CMAKE_CURRENT_SOURCE_DIR})
  if(${TEST_WITH_SLURM}) 
    add_test( ${test_exe} "srun" "${CMAKE_CURRENT_BINARY_DIR}/${test_exe}")
  else()
    add_test( ${test_exe} ${MPIEXEC} ${MPIEXEC_NUMPROC_FLAG}
    "4" ${MPIEXEC_PREFLAGS} "${CMAKE_CURRENT_BINARY_DIR}/${test_exe}")
  endif()
endfunction()


find_package(OpenMP)

#
# This function adds an MPI + OpenMP test.
#
function ( add_mpi_omp_test test_name )
  set(test_source "${test_name}.cpp")
  set(test_exe    "test_MPI_OMP_${test_name}")
  add_executable(${test_exe} ${test_source})
  include_link_mpi(${test_exe})
  target_link_libraries(${test_exe}  OpenMP::OpenMP_CXX)
  if(UNIX AND NOT APPLE)
    target_link_libraries(${test_exe} rt)
  endif()
  include_directories(${CMAKE_CURRENT_SOURCE_DIR})
  if(${TEST_WITH_SLURM}) 
    add_test( ${test_exe} "srun" "${CMAKE_CURRENT_BINARY_DIR}/${test_exe}")
  else()
    add_test( ${test_exe} ${MPIEXEC} ${MPIEXEC_NUMPROC_FLAG}
    "4" ${MPIEXEC_PREFLAGS} "${CMAKE_CURRENT_BINARY_DIR}/${test_exe}")
  endif()
endfunction()

add_seq_test(test_cereal_archive)

add_mpi_omp_test(test_comm)
add_mpi_omp_test(test_comm_2)
add_mpi_omp_test(test_large_messages)
add_mpi_omp_test(test_map)
add_mpi_omp_test(test_multimap)
add_mpi_omp_test(test_set)
add_mpi_omp_test(test_bag)
add_mpi_omp_test(test_multiset)
add_mpi_omp_test(test_counting_set)
add_mpi_omp_test(test_disjoint_set)
add_mpi_omp_test(test_container_serialization)

if (Boost_FOUND)
  add_seq_test(test_cereal_boost_json)
  target_include_directories(SEQ_test_cereal_boost_json PUBLIC ${Boost_INCLUDE_DIRS})
endif ()
