// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once
// #include <bit>
#include <functional>

namespace ygm::container::detail {

template <typename Key>
struct old_hash_partitioner {
  std::pair<size_t, size_t> operator()(const Key &k, size_t nranks,
                                       size_t nbanks) const {
    size_t hash = std::hash<Key>{}(k);
    size_t rank = hash % nranks;
    size_t bank = (hash / nranks) % nbanks;
    return std::make_pair(rank, bank);
  }
};

template <typename Hash>
struct hash_partitioner {
  hash_partitioner(ygm::comm &comm, Hash hash = Hash())
      : m_comm_size(comm.size()), m_hasher(hash) {}
  template <typename Key>
<<<<<<< HEAD
  int owner(const Key &key) {
    return (m_hash(key) * 2654435769L >> 32) % m_comm_size;
=======
  int owner(const Key &key) const {
    return (m_hasher(key) * 2654435769L >> 32) %
           m_comm_size;  // quick attempt to add salt to underlying hash
                         // function used by unordered_map
>>>>>>> b4ca7d43cd3ad6e8fcacf4d1cda0fdcb074ff7d8
  }

 private:
  int  m_comm_size;
  Hash m_hasher;
};

}  // namespace ygm::container::detail
