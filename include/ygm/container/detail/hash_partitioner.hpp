// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once
#include <functional>

namespace ygm::container::detail {

template <typename Key>
struct hash_partitioner {
  std::pair<size_t, size_t> operator()(const Key& k, size_t nranks,
                                       size_t nbanks) const {
    size_t hash = std::hash<Key>{}(k);
    size_t rank = hash % nranks;
    size_t bank = (hash / nranks) % nbanks;
    return std::make_pair(rank, bank);
  }
};

}  // namespace ygm::container::detail
