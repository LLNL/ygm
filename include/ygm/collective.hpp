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
inline bool logical_and(bool value, comm &c) {
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
inline bool logical_or(bool value, comm &c) {
  bool to_return;
  c.barrier();
  MPI_Comm mpi_comm = c.get_mpi_comm();
  ASSERT_MPI(MPI_Allreduce(&value, &to_return, 1, detail::mpi_typeof(bool()),
                           MPI_LOR, mpi_comm));
  return to_return;
}

/**
 * @brief Broadcasts to_bcast from root to all other ranks in communicator.
 *
 * @tparam T
 * @param to_bcast
 * @param root
 * @param cm
 */
template <typename T>
void bcast(T &to_bcast, int root, comm &cm) {
  if constexpr (std::is_trivially_copyable<T>::value &&
                std::is_standard_layout<T>::value) {
    ASSERT_MPI(
        MPI_Bcast(&to_bcast, sizeof(T), MPI_BYTE, root, cm.get_mpi_comm()));
  } else {
    std::vector<std::byte>   packed;
    cereal::YGMOutputArchive oarchive(packed);
    if (cm.rank() == root) {
      oarchive(to_bcast);
    }
    size_t packed_size = packed.size();
    ASSERT_RELEASE(packed_size < 1024 * 1024 * 1024);
    ASSERT_MPI(MPI_Bcast(&packed_size, 1, ygm::detail::mpi_typeof(packed_size),
                         root, cm.get_mpi_comm()));
    if (cm.rank() != root) {
      packed.resize(packed_size);
    }
    ASSERT_MPI(MPI_Bcast(packed.data(), packed_size, MPI_BYTE, root,
                         cm.get_mpi_comm()));

    if (cm.rank() != root) {
      cereal::YGMInputArchive iarchive(packed.data(), packed.size());
      iarchive(to_bcast);
    }
  }
}

template <typename T, typename Equal = std::equal_to<T>>
bool is_same(const T &to_check, comm &cm, const Equal &equals = Equal()) {
  T to_bcast;
  if (cm.rank() == 0) {
    to_bcast = to_check;
  }
  bcast(to_bcast, 0, cm);
  bool local_is_same = equals(to_check, to_bcast);
  return logical_and(local_is_same, cm);
}

}  // namespace ygm