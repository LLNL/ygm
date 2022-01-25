// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once

#include <exception>
#include <iostream>
#include <sstream>

#include <ygm/detail/mpi.hpp>

namespace ygm {

class Layout {
 private:
  // MPI_Comm m_comm_local;
  // MPI_Comm m_comm_node;
  int m_world_size;
  int m_world_rank;
  int m_local_size;
  int m_local_rank;
  int m_node_size;
  int m_node_rank;

  std::vector<int> m_local_ranks;
  std::vector<int> m_node_ranks;
  std::vector<int> m_rank_to_local;
  std::vector<int> m_rank_to_node;

  template <typename T>
  void mpi_allgather(T _t, std::vector<T> &out_vec, int size, MPI_Comm comm) {
    out_vec.resize(size);
    ASSERT_MPI(MPI_Allgather(&_t, sizeof(_t), MPI_BYTE, &(out_vec[0]),
                             sizeof(_t), MPI_BYTE, comm));
  }

 public:
  Layout(MPI_Comm comm) {
    // global ranks
    ASSERT_MPI(MPI_Comm_size(comm, &m_world_size));
    ASSERT_MPI(MPI_Comm_rank(comm, &m_world_rank));

    // local ranks
    MPI_Comm comm_local;
    ASSERT_MPI(MPI_Comm_split_type(comm, MPI_COMM_TYPE_SHARED, m_world_rank,
                                   MPI_INFO_NULL, &comm_local));
    ASSERT_MPI(MPI_Comm_size(comm_local, &m_local_size));
    ASSERT_MPI(MPI_Comm_rank(comm_local, &m_local_rank));

    mpi_allgather(m_local_rank, m_local_ranks, m_local_size, comm_local);

    // m_local_ranks.resize(m_local_size);
    // ASSERT_MPI(MPI_Allgather(&m_local_rank, 1, MPI_INT, &(m_local_ranks[0]),
    // 1,
    //                          MPI_INT, comm_local));

    // remote ranks
    MPI_Comm comm_node;
    ASSERT_MPI(MPI_Comm_split(comm, m_local_rank, m_world_rank, &comm_node));
    ASSERT_MPI(MPI_Comm_size(comm_node, &m_node_size));
    ASSERT_MPI(MPI_Comm_rank(comm_node, &m_node_rank));

    mpi_allgather(m_node_rank, m_node_ranks, m_node_size, comm_node);

    mpi_allgather(m_local_rank, m_rank_to_local, m_world_size, comm);
    mpi_allgather(m_node_rank, m_rank_to_node, m_world_size, comm);

    // m_node_ranks.resize(m_node_size);
    // ASSERT_MPI(MPI_Allgather(&m_node_ranks, 1, MPI_INT, &(m_node_ranks[0]),
    // 1,
    //                          MPI_INT, comm_local));
    // delete communicators
    ASSERT_RELEASE(MPI_Comm_free(&comm_local) == MPI_SUCCESS);
    ASSERT_RELEASE(MPI_Comm_free(&comm_node) == MPI_SUCCESS);
  }

  Layout(const Layout &rhs)
      : m_world_size(rhs.m_world_size),
        m_world_rank(rhs.m_world_rank),
        m_local_size(rhs.m_local_size),
        m_local_rank(rhs.m_local_rank),
        m_node_size(rhs.m_node_size),
        m_node_rank(rhs.m_node_rank) {
    // ASSERT_MPI(MPI_Comm_dup(rhs.m_comm_local, &m_comm_local));
    // ASSERT_MPI(MPI_Comm_dup(rhs.m_comm_node, &m_comm_node));
  }

  Layout() {}

  friend void swap(Layout &lhs, Layout &rhs) {
    std::swap(lhs.m_world_size, rhs.m_world_size);
    std::swap(lhs.m_world_rank, rhs.m_world_rank);
    std::swap(lhs.m_local_size, rhs.m_local_size);
    std::swap(lhs.m_local_rank, rhs.m_local_rank);
    std::swap(lhs.m_node_size, rhs.m_node_size);
    std::swap(lhs.m_node_rank, rhs.m_node_rank);
    std::swap(lhs.m_local_ranks, rhs.m_local_ranks);
    std::swap(lhs.m_node_ranks, rhs.m_node_ranks);
    std::swap(lhs.m_rank_to_node, rhs.m_rank_to_node);
    std::swap(lhs.m_rank_to_local, rhs.m_rank_to_local);
  }

  ~Layout() {
    // ASSERT_RELEASE(MPI_Comm_free(&m_local));
    // ASSERT_RELEASE(MPI_Comm_free(&m_node));
  }

  constexpr int count() const { return m_world_size; }
  constexpr int rank() const { return m_world_rank; }

  constexpr int node_count() const { return m_node_size; }
  constexpr int local_count() const { return m_local_size; }

  //////////////////////////////////////////////////////////////////////////////
  // ID conversion
  //////////////////////////////////////////////////////////////////////////////

  constexpr int node_id() const { return m_node_rank; }
  inline int    node_id(const int rank) const {
    _check_world_rank(rank);
    return m_rank_to_node.at(rank);
    // return rank / m_local_size;
  }

  constexpr int local_id() const { return m_local_rank; }
  inline int    local_id(const int rank) const {
    _check_world_rank(rank);
    return m_rank_to_local.at(rank);
    // return rank % m_local_size;
  }

  constexpr std::pair<int, int> rank_to_nl() const {
    return {m_node_rank, m_local_rank};
  }
  inline std::pair<int, int> rank_to_nl(const int rank) const {
    return {node_id(rank), local_id(rank)};
  }

  inline int nl_to_rank(const int nid, const int lid) const {
    _check_node_rank(nid);
    _check_local_rank(lid);
    return nid * m_local_size + lid;
  }
  inline int nl_to_rank(const std::pair<int, int> pid) const {
    return nl_to_rank(pid.first, pid.second);
  }

  inline bool is_local(const int rank) const {
    _check_local_rank(rank);
    return m_node_rank == node_id(rank);
  }

  //////////////////////////////////////////////////////////////////////////////
  // Cached data
  //////////////////////////////////////////////////////////////////////////////

  // constexpr const std::vector<int> &local_ranks() const {
  //   return m_local_ranks;
  // }
  // constexpr const std::vector<int> &node_ranks() const { return m_node_ranks;
  // } constexpr const std::vector<int> &rank_to_local() const {
  //   return m_rank_to_local;
  // }
  // constexpr const std::vector<int> &rank_to_node() const {
  //   return m_rank_to_node;
  // }

 private:
  inline void _check_world_rank(const int rank) const {
    _check_rank(rank, m_world_size, "world");
  }
  inline void _check_local_rank(const int local_rank) const {
    _check_rank(local_rank, m_local_size, "local");
  }
  inline void _check_node_rank(const int node_rank) const {
    _check_rank(node_rank, m_node_size, "node");
  }
  inline void _check_rank(const int rank, const int size,
                          const char *scope) const {
    if (rank < 0 || rank > m_world_size) {
      std::stringstream ss;
      ss << scope << " rank " << rank << " is not in the range [0, "
         << m_world_size << "]";
      throw std::logic_error(ss.str());
    }
  }
};

}  // namespace ygm