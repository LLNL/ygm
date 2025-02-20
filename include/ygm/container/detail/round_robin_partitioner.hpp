// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once

namespace ygm::container::detail {

struct round_robin_partitioner {
  round_robin_partitioner(ygm::comm &comm)
      : m_next(comm.rank()), m_comm_size(comm.size()) {}
  template <typename Item>
  int owner(const Item &) {
    if (++m_next >= m_comm_size) {
      m_next = 0;
    }
    return m_next;
  }
  int m_next;
  int m_comm_size;
};

}  // namespace ygm::container::detail