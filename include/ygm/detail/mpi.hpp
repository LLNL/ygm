// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once

#include <mpi.h>
#include <ygm/detail/assert.hpp>
#include <ygm/detail/ygm_traits.hpp>

namespace ygm::detail {
class mpi_init_finalize {
 public:
  mpi_init_finalize(int *argc, char ***argv) {
    YGM_ASSERT_MPI(MPI_Init(argc, argv));
  }
  ~mpi_init_finalize() {
    YGM_ASSERT_RELEASE(MPI_Barrier(MPI_COMM_WORLD) == MPI_SUCCESS);
    if (MPI_Finalize() != MPI_SUCCESS) {
      std::cerr << "ERROR:  MPI_Finilize() != MPI_SUCCESS" << std::endl;
      exit(-1);
    }
  }
};

template <typename T>
inline MPI_Datatype mpi_typeof(T) {
  static_assert(always_false<>, "Unkown MPI Type");
  return 0;
}

template <>
inline MPI_Datatype mpi_typeof<char>(char) {
  return MPI_CHAR;
}

template <>
inline MPI_Datatype mpi_typeof<bool>(bool) {
  return MPI_CXX_BOOL;
}

template <>
inline MPI_Datatype mpi_typeof<int8_t>(int8_t) {
  return MPI_INT8_T;
}

template <>
inline MPI_Datatype mpi_typeof<int16_t>(int16_t) {
  return MPI_INT16_T;
}

template <>
inline MPI_Datatype mpi_typeof<int32_t>(int32_t) {
  return MPI_INT32_T;
}

template <>
inline MPI_Datatype mpi_typeof<int64_t>(int64_t) {
  return MPI_INT64_T;
}

template <>
inline MPI_Datatype mpi_typeof<uint8_t>(uint8_t) {
  return MPI_UINT8_T;
}

template <>
inline MPI_Datatype mpi_typeof<uint16_t>(uint16_t) {
  return MPI_UINT16_T;
}

template <>
inline MPI_Datatype mpi_typeof<uint32_t>(uint32_t) {
  return MPI_UINT32_T;
}

template <>
inline MPI_Datatype mpi_typeof<uint64_t>(uint64_t) {
  return MPI_UINT64_T;
}

template <>
inline MPI_Datatype mpi_typeof<float>(float) {
  return MPI_FLOAT;
}

template <>
inline MPI_Datatype mpi_typeof<double>(double) {
  return MPI_DOUBLE;
}

template <>
inline MPI_Datatype mpi_typeof<long double>(long double) {
  return MPI_LONG_DOUBLE;
}

}  // namespace ygm::detail
