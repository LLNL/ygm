// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once

#include <ygm/comm.hpp>

namespace ygm {

/**
 * @brief Collective computes the prefix sum of value across all ranks in the
 * communicator.
 *
 * @tparam T
 * @param value
 * @param c
 * @return T
 */
template <typename T>
T prefix_sum(const T &value, comm &c) {
  T to_return{0};
  c.barrier();
  MPI_Comm mpi_comm = c.get_mpi_comm();
  ASSERT_MPI(MPI_Exscan(&value, &to_return, 1, detail::mpi_typeof(value),
                        MPI_SUM, mpi_comm));
  return to_return;
}

/**
 * @brief Collective computes the sum of value across all ranks in the
 * communicator.
 *
 * @tparam T
 * @param value
 * @param c
 * @return T
 */
template <typename T>
T sum(const T &value, comm &c) {
  T to_return;
  c.barrier();
  MPI_Comm mpi_comm = c.get_mpi_comm();
  ASSERT_MPI(MPI_Allreduce(&value, &to_return, 1, detail::mpi_typeof(T()),
                           MPI_SUM, mpi_comm));
  return to_return;
}

/**
 * @brief Collective computes the min of value across all ranks in the
 * communicator.
 *
 * @tparam T
 * @param value
 * @param c
 * @return T
 */
template <typename T>
T min(const T &value, comm &c) {
  T to_return;
  c.barrier();
  MPI_Comm mpi_comm = c.get_mpi_comm();
  ASSERT_MPI(MPI_Allreduce(&value, &to_return, 1, detail::mpi_typeof(T()),
                           MPI_MIN, mpi_comm));
  return to_return;
}

/**
 * @brief Collective computes the max of value across all ranks in the
 * communicator.
 *
 * @tparam T
 * @param value
 * @param c
 * @return T
 */
template <typename T>
T max(const T &value, comm &c) {
  T to_return;
  c.barrier();
  MPI_Comm mpi_comm = c.get_mpi_comm();
  ASSERT_MPI(MPI_Allreduce(&value, &to_return, 1, detail::mpi_typeof(T()),
                           MPI_MAX, mpi_comm));
  return to_return;
}

/**
 * @brief Collective computes the logical and of value across all ranks in the
 * communicator.
 *
 * @tparam T
 * @param value
 * @param c
 * @return T
 */
bool logical_and(bool value, comm &c) {
  bool to_return;
  c.barrier();
  MPI_Comm mpi_comm = c.get_mpi_comm();
  ASSERT_MPI(MPI_Allreduce(&value, &to_return, 1, detail::mpi_typeof(bool()),
                           MPI_LAND, mpi_comm));
  return to_return;
}

/**
 * @brief Collective computes the logical or of value across all ranks in the
 * communicator.
 *
 * @tparam T
 * @param value
 * @param c
 * @return T
 */
bool logical_or(bool value, comm &c) {
  bool to_return;
  c.barrier();
  MPI_Comm mpi_comm = c.get_mpi_comm();
  ASSERT_MPI(MPI_Allreduce(&value, &to_return, 1, detail::mpi_typeof(bool()),
                           MPI_LOR, mpi_comm));
  return to_return;
}

}  // namespace ygm