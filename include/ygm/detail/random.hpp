// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once

#include <random>
#include <unordered_set>
#include <vector>

namespace ygm {

/// @brief sample a set of `count` elements from the range `[lb,ub]` without
/// replacement. Based upon the algorithm by Robert Floyd.
/// @param lb the lower bound of the range
/// @param ub the upper bound of the range
/// @param count the number of samples to draw
/// @return returns a vector
std::vector<std::size_t> random_subset(std::size_t lb, std::size_t ub,
                                       std::size_t count) {
  ASSERT_RELEASE(count < ub - lb);
  std::unordered_set<int> samples;
  std::random_device      rd;
  std::mt19937            gen(rd());

  for (int alternative(ub - count); alternative < ub; ++alternative) {
    std::size_t candidate =
        std::uniform_int_distribution<std::size_t>(lb, alternative)(gen);
    if (samples.find(candidate) == std::end(samples)) {
      samples.insert(candidate);
    } else {
      samples.insert(alternative);
    }
  }
  std::vector<std::size_t> vec;
  vec.reserve(count);
  for (auto it(std::begin(samples)); it != std::end(samples); ++it) {
    // vec.push_back(std::move(samples.extract(it).value()));
    vec.push_back(*it);
  }
  return vec;
}
}  // namespace ygm
