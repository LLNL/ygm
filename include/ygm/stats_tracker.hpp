// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once

#include <functional>
#include <iomanip>

#include <ygm/detail/distributed_string_enumeration.hpp>
#include <ygm/detail/distributed_string_literal_map.hpp>
#include <ygm/detail/string_enumerator.hpp>
#include <ygm/detail/string_literal_map.hpp>
#include <ygm/utility.hpp>

namespace ygm {
class stats_tracker {
 public:
  stats_tracker(ygm::comm &comm) : m_comm(comm) {}

  template <ygm::detail::StringLiteral S>
  void start_timer() {
    m_timers.get_value<S>().first.reset();
  }

  template <ygm::detail::StringLiteral S>
  void stop_timer() {
    auto &[timer, time] = m_timers.get_value<S>();
    time += timer.elapsed();
  }

  template <ygm::detail::StringLiteral S, typename Function>
  size_t get_time(Function fn) {
    return fn(m_timers.get_value<S>().second);
  }

  template <ygm::detail::StringLiteral S>
  double get_time_local() {
    auto identity_lambda = [](const auto time) { return time; };
    return get_time<S>(identity_lambda);
  }

  template <ygm::detail::StringLiteral S>
  double get_time_max() {
    auto max_lambda = [this](const auto time) {
      return ygm::max(time, this->m_comm);
    };
    return get_time<S>(max_lambda);
  }

  template <ygm::detail::StringLiteral S>
  double get_time_min() {
    auto min_lambda = [this](const auto time) {
      return ygm::min(time, this->m_comm);
    };
    return get_time<S>(min_lambda);
  }

  template <ygm::detail::StringLiteral S>
  double get_time_sum() {
    auto sum_lambda = [this](const auto time) {
      return ygm::sum(time, this->m_comm);
    };
    return get_time<S>(sum_lambda);
  }

  template <ygm::detail::StringLiteral S>
  double get_time_avg() {
    return get_time_sum<S>() / m_comm.size();
  }

  template <ygm::detail::StringLiteral S>
  void increment_counter(size_t summand = 1) {
    m_counters.get_value<S>() += summand;
  }

  template <ygm::detail::StringLiteral S, typename Function>
  size_t get_counter(Function fn) {
    return fn(m_counters.get_value<S>());
  }

  template <ygm::detail::StringLiteral S>
  size_t get_counter_local() {
    auto identity_lambda = [](const auto time) { return time; };
    return get_counter<S>(identity_lambda);
  }

  template <ygm::detail::StringLiteral S>
  size_t get_counter_max() {
    auto max_lambda = [this](const auto count) {
      return ygm::max(count, this->m_comm);
    };
    return get_counter<S>(max_lambda);
  }

  template <ygm::detail::StringLiteral S>
  size_t get_counter_min() {
    auto min_lambda = [this](const auto count) {
      return ygm::min(count, this->m_comm);
    };
    return get_counter<S>(min_lambda);
  }

  template <ygm::detail::StringLiteral S>
  size_t get_counter_sum() {
    auto sum_lambda = [this](const auto count) {
      return ygm::sum(count, this->m_comm);
    };
    return get_counter<S>(sum_lambda);
  }

  template <ygm::detail::StringLiteral S>
  double get_counter_avg() {
    return get_counter_sum<S>() / ((double)m_comm.size());
  }

  void print(const std::string &name = "", std::ostream &os = std::cout) {
    // This will all be much easier with std::format in C++23...
    std::stringstream sstr;
    constexpr int     number_field_width = 16;
    constexpr int     name_width         = 24;
    constexpr int     total_row_length   = 4 * number_field_width + name_width;

    if (name.size() > 0) {
      int filler_length =
          std::max<int>(total_row_length - 7 - (name.size() + 1), 0);
      sstr << std::string(filler_length / 2, '=') << " " << name << " STATS "
           << std::string(filler_length / 2 + (filler_length % 2), '=') << "\n";
    } else {
      int filler_length = std::max<int>(total_row_length - 7, 0);
      sstr << std::string(filler_length / 2, '=') << " STATS "
           << std::string(filler_length / 2 + (filler_length % 2), '=') << "\n";
    }

    sstr << std::string(name_width, ' ');
    sstr << std::string(number_field_width - 5, ' ') << "(min)";
    sstr << std::string(number_field_width - 5, ' ') << "(max)";
    sstr << std::string(number_field_width - 5, ' ') << "(sum)";
    sstr << std::string(number_field_width - 9, ' ') << "(average)";
    sstr << "\n";

    // Print timers
    ygm::detail::string_literal_map_match_keys(m_counters, m_comm);
    for (auto &&timer : m_timers) {
      const auto &name          = timer.first;
      int         filler_length = std::max<int>(name_width - name.size(), 0);
      sstr << std::string(filler_length, ' ')
           << std::setw(name_width - filler_length)
           << name.substr(0, name_width - filler_length);

      const auto min = ygm::min(timer.second.second, m_comm);
      sstr << std::setw(number_field_width) << min;

      const auto max = ygm::max(timer.second.second, m_comm);
      sstr << std::setw(number_field_width) << max;

      const auto sum = ygm::sum(timer.second.second, m_comm);
      sstr << std::setw(number_field_width) << sum;

      const auto avg = ygm::sum(timer.second.second, m_comm) / m_comm.size();
      sstr << std::setw(number_field_width) << avg;

      sstr << "\n";
    }

    // Print counters
    ygm::detail::string_literal_map_match_keys(m_timers, m_comm);
    for (auto &&counter : m_counters) {
      const auto &name          = counter.first;
      int         filler_length = std::max<int>(name_width - name.size(), 0);
      sstr << std::string(filler_length, ' ')
           << std::setw(name_width - filler_length)
           << name.substr(0, name_width - filler_length);

      const auto min = ygm::min(counter.second, m_comm);
      sstr << std::setw(number_field_width) << min;

      const auto max = ygm::max(counter.second, m_comm);
      sstr << std::setw(number_field_width) << max;

      const auto sum = ygm::sum(counter.second, m_comm);
      sstr << std::setw(number_field_width) << sum;

      const auto avg =
          ((double)ygm::sum(counter.second, m_comm)) / m_comm.size();
      sstr << std::setw(number_field_width) << avg;

      sstr << "\n";
    }

    sstr << std::string(total_row_length, '=');

    if (m_comm.rank0()) {
      os << sstr.str() << std::endl;
    }
  }

 private:
  ygm::comm &m_comm;

  ygm::detail::string_literal_map<std::pair<ygm::timer, double>> m_timers;
  ygm::detail::string_literal_map<size_t>                        m_counters;
};
}  // namespace ygm
