// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once
#include <set>
#include <ygm/comm.hpp>

namespace ygm::container {

template <typename Key, typename Partitioner, typename Compare, class Alloc>
set<Key, Partitioner, Compare, Alloc>::set(ygm::comm &comm)
    : m_comm(comm), pthis(this) {
  pthis.check(m_comm);
}

template <typename Key, typename Partitioner, typename Compare, class Alloc>
set<Key, Partitioner, Compare, Alloc>::set(const self_type &&rhs) noexcept
    : m_comm(rhs.m_comm), pthis(this), m_local_set(std::move(rhs.m_local_set)) {
  pthis.check(m_comm);
}

template <typename Key, typename Partitioner, typename Compare, class Alloc>
set<Key, Partitioner, Compare, Alloc>::~set() {
  m_comm.barrier();
}

template <typename Key, typename Partitioner, typename Compare, class Alloc>
void set<Key, Partitioner, Compare, Alloc>::async_insert(const key_type &key) {
  auto inserter = [](auto pset, const key_type &key) {
    pset->m_local_set.insert(key);
  };
  int dest = owner(key);
  m_comm.async(dest, inserter, pthis, key);
}

template <typename Key, typename Partitioner, typename Compare, class Alloc>
void set<Key, Partitioner, Compare, Alloc>::async_erase(const key_type &key) {
  int  dest          = owner(key);
  auto erase_wrapper = [](auto pset, const key_type &key) {
    pset->m_local_set.erase(key);
  };

  m_comm.async(dest, erase_wrapper, pthis, key);
}

template <typename Key, typename Partitioner, typename Compare, class Alloc>
template <typename Visitor, typename... VisitorArgs>
void set<Key, Partitioner, Compare, Alloc>::async_insert_exe_if_missing(
    const key_type &key, Visitor visitor, const VisitorArgs &...args) {
  auto insert_and_visit = [](auto pset, const key_type &key,
                             const VisitorArgs &...args) {
    if (pset->m_local_set.count(key) == 0) {
      pset->m_local_set.insert(key);
      Visitor *vis = nullptr;
      std::apply(*vis, std::forward_as_tuple(key, args...));
    }
  };
  int dest = owner(key);
  m_comm.async(dest, insert_and_visit, pthis, key,
               std::forward<const VisitorArgs>(args)...);
}

template <typename Key, typename Partitioner, typename Compare, class Alloc>
template <typename Visitor, typename... VisitorArgs>
void set<Key, Partitioner, Compare, Alloc>::async_insert_exe_if_contains(
    const key_type &key, Visitor visitor, const VisitorArgs &...args) {
  auto insert_and_visit = [](auto pset, const key_type &key,
                             const VisitorArgs &...args) {
    if (pset->m_local_set.count(key) == 0) {
      pset->m_local_set.insert(key);
    } else {
      Visitor *vis = nullptr;
      std::apply(*vis, std::forward_as_tuple(key, args...));
    }
  };
  int dest = owner(key);
  m_comm.async(dest, insert_and_visit, pthis, key,
               std::forward<const VisitorArgs>(args)...);
}

template <typename Key, typename Partitioner, typename Compare, class Alloc>
template <typename Visitor, typename... VisitorArgs>
void set<Key, Partitioner, Compare, Alloc>::async_exe_if_missing(
    const key_type &key, Visitor visitor, const VisitorArgs &...args) {
  auto checker = [](auto pset, const key_type &key,
                    const VisitorArgs &...args) {
    if (pset->m_local_set.count(key) == 0) {
      Visitor *vis = nullptr;
      std::apply(*vis, std::forward_as_tuple(key, args...));
    }
  };
  int dest = owner(key);
  m_comm.async(dest, checker, pthis, key,
               std::forward<const VisitorArgs>(args)...);
}

template <typename Key, typename Partitioner, typename Compare, class Alloc>
template <typename Visitor, typename... VisitorArgs>
void set<Key, Partitioner, Compare, Alloc>::async_exe_if_contains(
    const key_type &key, Visitor visitor, const VisitorArgs &...args) {
  auto checker = [](auto pset, const key_type &key,
                    const VisitorArgs &...args) {
    if (pset->m_local_set.count(key) == 1) {
      Visitor *vis = nullptr;
      std::apply(*vis, std::forward_as_tuple(key, args...));
    }
  };
  int dest = owner(key);
  m_comm.async(dest, checker, pthis, key,
               std::forward<const VisitorArgs>(args)...);
}

template <typename Key, typename Partitioner, typename Compare, class Alloc>
template <typename Function>
void set<Key, Partitioner, Compare, Alloc>::for_all(Function fn) {
  m_comm.barrier();
  local_for_all(fn);
}

template <typename Key, typename Partitioner, typename Compare, class Alloc>
template <typename Function>
void set<Key, Partitioner, Compare, Alloc>::consume_all(Function fn) {
  m_comm.barrier();
  local_consume_all(fn);
}

template <typename Key, typename Partitioner, typename Compare, class Alloc>
void set<Key, Partitioner, Compare, Alloc>::clear() {
  m_comm.barrier();
  m_local_set.clear();
}

template <typename Key, typename Partitioner, typename Compare, class Alloc>
typename set<Key, Partitioner, Compare, Alloc>::size_type
set<Key, Partitioner, Compare, Alloc>::size() {
  m_comm.barrier();
  return m_comm.all_reduce_sum(m_local_set.size());
}

template <typename Key, typename Partitioner, typename Compare, class Alloc>
typename set<Key, Partitioner, Compare, Alloc>::size_type
set<Key, Partitioner, Compare, Alloc>::count(const key_type &key) {
  m_comm.barrier();
  return m_comm.all_reduce_sum(m_local_set.count(key));
}

// Doesn't swap pthis.
// should we check comm is equal? -- probably
template <typename Key, typename Partitioner, typename Compare, class Alloc>
void set<Key, Partitioner, Compare, Alloc>::swap(self_type &s) {
  m_comm.barrier();
  m_local_set.swap(s.m_local_set);
}

template <typename Key, typename Partitioner, typename Compare, class Alloc>
typename set<Key, Partitioner, Compare, Alloc>::ptr_type
set<Key, Partitioner, Compare, Alloc>::get_ygm_ptr() const {
  return pthis;
}

template <typename Key, typename Partitioner, typename Compare, class Alloc>
void set<Key, Partitioner, Compare, Alloc>::serialize(
    const std::string &fname) {
  m_comm.barrier();
  std::string               rank_fname = fname + std::to_string(m_comm.rank());
  std::ofstream             os(rank_fname, std::ios::binary);
  cereal::JSONOutputArchive oarchive(os);
  oarchive(m_local_set, m_comm.size());
}

template <typename Key, typename Partitioner, typename Compare, class Alloc>
void set<Key, Partitioner, Compare, Alloc>::deserialize(
    const std::string &fname) {
  m_comm.barrier();

  std::string   rank_fname = fname + std::to_string(m_comm.rank());
  std::ifstream is(rank_fname, std::ios::binary);

  cereal::JSONInputArchive iarchive(is);
  int                      comm_size;
  iarchive(m_local_set, comm_size);

  if (comm_size != m_comm.size()) {
    m_comm.cerr0(
        "Attempting to deserialize set using communicator of "
        "different size than serialized with");
  }
}

template <typename Key, typename Partitioner, typename Compare, class Alloc>
template <typename Function>
void set<Key, Partitioner, Compare, Alloc>::local_for_all(Function fn) {
  if constexpr (std::is_invocable<decltype(fn), const key_type &>()) {
    std::for_each(m_local_set.begin(), m_local_set.end(), fn);
  } else {
    static_assert(ygm::detail::always_false<>,
                  "local set lambda signature must be invocable with (const "
                  "key_type &) signature");
  }
}

template <typename Key, typename Partitioner, typename Compare, class Alloc>
template <typename Function>
void set<Key, Partitioner, Compare, Alloc>::local_consume_all(Function fn) {
  if constexpr (std::is_invocable<decltype(fn), const key_type &>()) {
    while (!m_local_set.empty()) {
      auto tmp = *(m_local_set.begin());
      m_local_set.erase(m_local_set.begin());
      fn(tmp);
    }
  } else {
    static_assert(ygm::detail::always_false<>,
                  "local set lambda signature must be invocable with (const "
                  "key_type &) signature");
  }
}

template <typename Key, typename Partitioner, typename Compare, class Alloc>
ygm::comm &set<Key, Partitioner, Compare, Alloc>::comm() {
  return m_comm;
}

template <typename Key, typename Partitioner, typename Compare, class Alloc>
int set<Key, Partitioner, Compare, Alloc>::owner(const key_type &key) const {
  auto [owner, rank] = partitioner(key, m_comm.size(), 1024);
  return owner;
}

template <typename Key, typename Partitioner, typename Compare, class Alloc>
multiset<Key, Partitioner, Compare, Alloc>::multiset(ygm::comm &comm)
    : m_comm(comm), pthis(this) {
  pthis.check(m_comm);
}

template <typename Key, typename Partitioner, typename Compare, class Alloc>
multiset<Key, Partitioner, Compare, Alloc>::multiset(
    const self_type &&rhs) noexcept
    : m_comm(rhs.m_comm), pthis(this), m_local_set(std::move(rhs.m_local_set)) {
  pthis.check(m_comm);
}

template <typename Key, typename Partitioner, typename Compare, class Alloc>
multiset<Key, Partitioner, Compare, Alloc>::~multiset() {
  m_comm.barrier();
}

template <typename Key, typename Partitioner, typename Compare, class Alloc>
void multiset<Key, Partitioner, Compare, Alloc>::async_insert(
    const key_type &key) {
  auto inserter = [](auto pset, const key_type &key) {
    pset->m_local_set.insert(key);
  };
  int dest = owner(key);
  m_comm.async(dest, inserter, pthis, key);
}

template <typename Key, typename Partitioner, typename Compare, class Alloc>
void multiset<Key, Partitioner, Compare, Alloc>::async_erase(
    const key_type &key) {
  int  dest          = owner(key);
  auto erase_wrapper = [](auto pset, const key_type &key) {
    pset->m_local_set.erase(key);
  };

  m_comm.async(dest, erase_wrapper, pthis, key);
}

template <typename Key, typename Partitioner, typename Compare, class Alloc>
template <typename Visitor, typename... VisitorArgs>
void multiset<Key, Partitioner, Compare, Alloc>::async_insert_exe_if_missing(
    const key_type &key, Visitor visitor, const VisitorArgs &...args) {
  auto insert_and_visit = [](auto pset, const key_type &key,
                             const VisitorArgs &...args) {
    if (pset->m_local_set.count(key) == 0) {
      pset->m_local_set.insert(key);
      Visitor *vis = nullptr;
      std::apply(*vis, std::forward_as_tuple(key, args...));
    }
  };
  int dest = owner(key);
  m_comm.async(dest, insert_and_visit, pthis, key,
               std::forward<const VisitorArgs>(args)...);
}

template <typename Key, typename Partitioner, typename Compare, class Alloc>
template <typename Visitor, typename... VisitorArgs>
void multiset<Key, Partitioner, Compare, Alloc>::async_insert_exe_if_contains(
    const key_type &key, Visitor visitor, const VisitorArgs &...args) {
  auto insert_and_visit = [](auto pset, const key_type &key,
                             const VisitorArgs &...args) {
    if (pset->m_local_set.count(key) == 0) {
      pset->m_local_set.insert(key);
    } else {
      Visitor *vis = nullptr;
      std::apply(*vis, std::forward_as_tuple(key, args...));
    }
  };
  int dest = owner(key);
  m_comm.async(dest, insert_and_visit, pthis, key,
               std::forward<const VisitorArgs>(args)...);
}

template <typename Key, typename Partitioner, typename Compare, class Alloc>
template <typename Visitor, typename... VisitorArgs>
void multiset<Key, Partitioner, Compare, Alloc>::async_exe_if_missing(
    const key_type &key, Visitor visitor, const VisitorArgs &...args) {
  auto checker = [](auto pset, const key_type &key,
                    const VisitorArgs &...args) {
    if (pset->m_local_set.count(key) == 0) {
      Visitor *vis = nullptr;
      std::apply(*vis, std::forward_as_tuple(key, args...));
    }
  };
  int dest = owner(key);
  m_comm.async(dest, checker, pthis, key,
               std::forward<const VisitorArgs>(args)...);
}

template <typename Key, typename Partitioner, typename Compare, class Alloc>
template <typename Visitor, typename... VisitorArgs>
void multiset<Key, Partitioner, Compare, Alloc>::async_exe_if_contains(
    const key_type &key, Visitor visitor, const VisitorArgs &...args) {
  auto checker = [](auto pset, const key_type &key,
                    const VisitorArgs &...args) {
    if (pset->m_local_set.count(key) == 1) {
      Visitor *vis = nullptr;
      std::apply(*vis, std::forward_as_tuple(key, args...));
    }
  };
  int dest = owner(key);
  m_comm.async(dest, checker, pthis, key,
               std::forward<const VisitorArgs>(args)...);
}

template <typename Key, typename Partitioner, typename Compare, class Alloc>
template <typename Function>
void multiset<Key, Partitioner, Compare, Alloc>::for_all(Function fn) {
  m_comm.barrier();
  local_for_all(fn);
}

template <typename Key, typename Partitioner, typename Compare, class Alloc>
template <typename Function>
void multiset<Key, Partitioner, Compare, Alloc>::consume_all(Function fn) {
  m_comm.barrier();
  local_consume_all(fn);
}

template <typename Key, typename Partitioner, typename Compare, class Alloc>
void multiset<Key, Partitioner, Compare, Alloc>::clear() {
  m_comm.barrier();
  m_local_set.clear();
}

template <typename Key, typename Partitioner, typename Compare, class Alloc>
typename multiset<Key, Partitioner, Compare, Alloc>::size_type
multiset<Key, Partitioner, Compare, Alloc>::size() {
  m_comm.barrier();
  return m_comm.all_reduce_sum(m_local_set.size());
}

template <typename Key, typename Partitioner, typename Compare, class Alloc>
typename multiset<Key, Partitioner, Compare, Alloc>::size_type
multiset<Key, Partitioner, Compare, Alloc>::count(const key_type &key) {
  m_comm.barrier();
  return m_comm.all_reduce_sum(m_local_set.count(key));
}

// Doesn't swap pthis.
// should we check comm is equal? -- probably
template <typename Key, typename Partitioner, typename Compare, class Alloc>
void multiset<Key, Partitioner, Compare, Alloc>::swap(self_type &s) {
  m_comm.barrier();
  m_local_set.swap(s.m_local_set);
}

template <typename Key, typename Partitioner, typename Compare, class Alloc>
typename multiset<Key, Partitioner, Compare, Alloc>::ptr_type
multiset<Key, Partitioner, Compare, Alloc>::get_ygm_ptr() const {
  return pthis;
}

template <typename Key, typename Partitioner, typename Compare, class Alloc>
void multiset<Key, Partitioner, Compare, Alloc>::serialize(
    const std::string &fname) {
  m_comm.barrier();
  std::string               rank_fname = fname + std::to_string(m_comm.rank());
  std::ofstream             os(rank_fname, std::ios::binary);
  cereal::JSONOutputArchive oarchive(os);
  oarchive(m_local_set, m_comm.size());
}

template <typename Key, typename Partitioner, typename Compare, class Alloc>
void multiset<Key, Partitioner, Compare, Alloc>::deserialize(
    const std::string &fname) {
  m_comm.barrier();

  std::string   rank_fname = fname + std::to_string(m_comm.rank());
  std::ifstream is(rank_fname, std::ios::binary);

  cereal::JSONInputArchive iarchive(is);
  int                      comm_size;
  iarchive(m_local_set, comm_size);

  if (comm_size != m_comm.size()) {
    m_comm.cerr0(
        "Attempting to deserialize multiset using communicator of "
        "different size than serialized with");
  }
}

template <typename Key, typename Partitioner, typename Compare, class Alloc>
template <typename Function>
void multiset<Key, Partitioner, Compare, Alloc>::local_for_all(Function fn) {
  if constexpr (std::is_invocable<decltype(fn), const key_type &>()) {
    std::for_each(m_local_set.begin(), m_local_set.end(), fn);
  } else {
    static_assert(ygm::detail::always_false<>,
                  "local set lambda signature must be invocable with (const "
                  "key_type &) signature");
  }
}

template <typename Key, typename Partitioner, typename Compare, class Alloc>
template <typename Function>
void multiset<Key, Partitioner, Compare, Alloc>::local_consume_all(
    Function fn) {
  if constexpr (std::is_invocable<decltype(fn), const key_type &>()) {
    while (!m_local_set.empty()) {
      auto tmp = *(m_local_set.begin());
      m_local_set.erase(m_local_set.begin());
      fn(tmp);
    }
  } else {
    static_assert(ygm::detail::always_false<>,
                  "local set lambda signature must be invocable with (const "
                  "key_type &) signature");
  }
}

template <typename Key, typename Partitioner, typename Compare, class Alloc>
ygm::comm &multiset<Key, Partitioner, Compare, Alloc>::comm() {
  return m_comm;
}

template <typename Key, typename Partitioner, typename Compare, class Alloc>
int multiset<Key, Partitioner, Compare, Alloc>::owner(
    const key_type &key) const {
  auto [owner, rank] = partitioner(key, m_comm.size(), 1024);
  return owner;
}
}  // namespace ygm::container
