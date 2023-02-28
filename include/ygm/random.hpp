// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once

#include <ygm/comm.hpp>

#include <random>

namespace ygm {
namespace random {

/// @brief Applies a simple offset to the specified seed according to rank index
/// @tparam ResultType The random number (seed) type
/// @param comm The ygm::comm to be used
/// @param seed The specified seed
/// @return simply returns seed + rank
template <typename ResultType>
ResultType simple_offset(ygm::comm &comm, ResultType seed) {
  return seed + comm.rank();
}

/// @brief Applies no change to the specified seed
/// @tparam ResultType The random number (seed) type
/// @param comm The ygm::comm to be used
/// @param seed The specified seed
/// @return simply returns the unmodified seed
template <typename ResultType>
ResultType no_offset(ygm::comm &comm, ResultType seed) {
  return seed;
}

/// @brief A wrapper around a per-rank random engine that manipulates each
///        rank's seed according to a specified strategy
/// @tparam RandomEngine The underlying random engine, e.g. std::mt19337
/// @tparam Function A function on (ygm::comm, result_type) -> result_type that
///         modifies seeds for each rank
template <typename RandomEngine,
          typename RandomEngine::result_type (*Function)(
              ygm::comm &, typename RandomEngine::result_type)>
class random_number_generator {
 public:
  using rng_type    = RandomEngine;
  using result_type = typename RandomEngine::result_type;

  random_number_generator(ygm::comm  &comm,
                          result_type seed = std::random_device{}())
      : m_seed(Function(comm, seed)), m_gen(Function(comm, seed)) {}

  result_type operator()() { return m_gen(); }

  constexpr const result_type &seed() const { return m_seed; }

  static constexpr result_type min() { return rng_type::min(); }
  static constexpr result_type max() { return rng_type::max(); }

 private:
  rng_type    m_gen;
  result_type m_seed;
};

/// @brief A simple offset rng alias
/// @tparam RandomEngine The underlying random engine, e.g. std::mt19337
template <typename RandomEngine>
using simple_offset_rng = random_number_generator<RandomEngine, simple_offset>;

/// @brief A shared rng alias, where each rank has the same seed
/// @tparam RandomEngine The underlying random engine, e.g. std::mt19337
template <typename RandomEngine>
using shared_rng = random_number_generator<RandomEngine, no_offset>;

}  // namespace random
}  // namespace ygm