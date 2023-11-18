#pragma once

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
    return get_time<S>(std::identity());
  }

  template <ygm::detail::StringLiteral S>
  double get_time_max() {
    auto max_lambda = [this](const auto time) {
      return max(time, this->m_comm);
    };
    return get_time<S>(max_lambda);
  }

  template <ygm::detail::StringLiteral S>
  double get_time_min() {
    auto min_lambda = [this](const auto time) {
      return min(time, this->m_comm);
    };
    return get_time<S>(min_lambda);
  }

  template <ygm::detail::StringLiteral S>
  double get_time_sum() {
    auto sum_lambda = [this](const auto time) {
      return sum(time, this->m_comm);
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
    return get_counter<S>(std::identity());
  }

  template <ygm::detail::StringLiteral S>
  size_t get_counter_max() {
    auto max_lambda = [this](const auto count) {
      return max(count, this->m_comm);
    };
    return get_counter<S>(max_lambda);
  }

  template <ygm::detail::StringLiteral S>
  size_t get_counter_min() {
    auto min_lambda = [this](const auto count) {
      return min(count, this->m_comm);
    };
    return get_counter<S>(min_lambda);
  }

  template <ygm::detail::StringLiteral S>
  size_t get_counter_sum() {
    auto sum_lambda = [this](const auto count) {
      return sum(count, this->m_comm);
    };
    return get_counter<S>(sum_lambda);
  }

  template <ygm::detail::StringLiteral S>
  double get_counter_avg() {
    return get_counter_sum<S>() / ((double)m_comm.size());
  }

  void print() {
    ygm::detail::string_literal_map_match_keys(m_counters, m_comm);
    ygm::detail::string_literal_map_match_keys(m_timers, m_comm);
  }

 private:
  ygm::comm &m_comm;

  ygm::detail::string_literal_map<std::pair<ygm::timer, double>> m_timers;
  ygm::detail::string_literal_map<size_t>                        m_counters;
};
}  // namespace ygm
