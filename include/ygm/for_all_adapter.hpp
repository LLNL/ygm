// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once

namespace ygm {

/**
 * @brief Consuming for_all adapter.
 *
 * @tparam Container
 */
template <typename Container>
class for_all_consume_adapter {
 public:
  for_all_consume_adapter(Container& c) : m_rc(c) {}

  template <typename Function>
  void for_all(Function fn) {
    m_rc.consume_all(fn);
  }

 private:
  Container& m_rc;
};

/**
 * @brief Adapter that iteratively calls consume_all until container is globally
 * empty.
 *
 * @tparam Container
 */
template <typename Container>
class consume_all_iterative_adapter {
 public:
  consume_all_iterative_adapter(Container& c) : m_rc(c) {}

  template <typename Function>
  void consume_all(Function fn) {
    while (not m_rc.empty()) {
      m_rc.consume_all(fn);
    }
  }

 private:
  Container& m_rc;
};
}  // namespace ygm