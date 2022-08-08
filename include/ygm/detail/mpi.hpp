// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once

#include <mpi.h>
#include <ygm/detail/assert.hpp>

namespace ygm::detail {
class mpi_init_finalize {
 public:
  mpi_init_finalize(int *argc, char ***argv) {
    int provided;
    ASSERT_MPI(MPI_Init_thread(argc, argv, MPI_THREAD_MULTIPLE, &provided));
    if (provided != MPI_THREAD_MULTIPLE) {
      throw std::runtime_error(
          "MPI_Init_thread: MPI_THREAD_MULTIPLE not provided.");
    }
  }
  ~mpi_init_finalize() {
    ASSERT_RELEASE(MPI_Barrier(MPI_COMM_WORLD) == MPI_SUCCESS);
    if (MPI_Finalize() != MPI_SUCCESS) {
      std::cerr << "ERROR:  MPI_Finilize() != MPI_SUCCESS" << std::endl;
      exit(-1);
    }
  }
};

inline MPI_Datatype mpi_typeof(char) { return MPI_CHAR; }
inline MPI_Datatype mpi_typeof(signed short) { return MPI_SHORT; }
inline MPI_Datatype mpi_typeof(signed int) { return MPI_INT; }
inline MPI_Datatype mpi_typeof(signed long) { return MPI_LONG; }
inline MPI_Datatype mpi_typeof(unsigned char) { return MPI_UNSIGNED_CHAR; }
inline MPI_Datatype mpi_typeof(unsigned short) { return MPI_UNSIGNED_SHORT; }
inline MPI_Datatype mpi_typeof(unsigned) { return MPI_UNSIGNED; }
inline MPI_Datatype mpi_typeof(unsigned long) { return MPI_UNSIGNED_LONG; }
inline MPI_Datatype mpi_typeof(unsigned long long) {
  return MPI_UNSIGNED_LONG_LONG;
}
inline MPI_Datatype mpi_typeof(signed long long) { return MPI_LONG_LONG_INT; }
inline MPI_Datatype mpi_typeof(float) { return MPI_FLOAT; }
inline MPI_Datatype mpi_typeof(double) { return MPI_DOUBLE; }
inline MPI_Datatype mpi_typeof(long double) { return MPI_LONG_DOUBLE; }

}  // namespace ygm::detail
