// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once

#include <mpi.h>

#include <ygm/detail/string_literal_map.hpp>
#include <ygm/utility.hpp>

namespace ygm {
namespace detail {
class comm_stats {
 public:
  comm_stats() {}

  void reset() {
    for (auto&& timer : m_timers) {
      timer.second.second = 0.0;
    }

    for (auto&& counter : m_counters) {
      counter.second = 0;
    }
  }

  template <StringLiteral S>
  void start_timer() {
    m_timers.get_value<S>().first.reset();
  }

  template <StringLiteral S>
  void stop_timer() {
    auto& [timer, time] = m_timers.get_value<S>();
    time += timer.elapsed();
  }

  template <StringLiteral S>
  void increment_counter(size_t summand = 1) {
    m_counters.get_value<S>() += summand;
  }

  string_literal_map<std::pair<ygm::timer, double>>& get_timers() {
    return m_timers;
  }
  string_literal_map<size_t>& get_counters() { return m_counters; }

 public:
  string_literal_map<std::pair<ygm::timer, double>> m_timers;
  string_literal_map<size_t>                        m_counters;
};
}  // namespace detail
}  // namespace ygm
