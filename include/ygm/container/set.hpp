// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once
#include <ygm/container/detail/set_impl.hpp>

namespace ygm::container {

template <
    typename Key, typename SendBufferManager = ygm::locking_send_buffer_manager,
    typename Partitioner = detail::hash_partitioner<Key>,
    typename Compare = std::less<Key>, class Alloc = std::allocator<const Key>,
    int NumBanks = 1024, typename LockBankTag = ygm::DefaultLockBankTag>
class multiset {
public:
  using self_type =
      multiset<Key, SendBufferManager, Partitioner, Compare, Alloc>;
  using key_type = Key;
  using impl_type = detail::set_impl<key_type, SendBufferManager, Partitioner,
                                     Compare, Alloc, NumBanks, LockBankTag>;

  Partitioner partitioner;

  multiset() = delete;

  multiset(ygm::comm<SendBufferManager> &comm) : m_impl(comm) {}

  void async_insert(const key_type &key) { m_impl.async_insert_multi(key); }

  void async_erase(const key_type &key) { m_impl.async_erase(key); }

  template <typename Function> void for_all(Function fn) { m_impl.for_all(fn); }

  void clear() { m_impl.clear(); }

  size_t size() { return m_impl.size(); }

  size_t count(const key_type &key) { return m_impl.count(key); }

  void swap(self_type &s) { return m_impl.swap(s.m_impl); }

  void serialize(const std::string &fname) { m_impl.serialize(fname); }
  void deserialize(const std::string &fname) { m_impl.deserialize(fname); }

  typename ygm::ygm_ptr<self_type> get_ygm_ptr() const {
    return m_impl.get_ygm_ptr();
  }

  template <typename Function> void local_for_all(Function fn) {
    m_impl.local_for_all(fn);
  }

  int owner(const key_type &key) const { return m_impl.owner(key); }

private:
  impl_type m_impl;
};
template <
    typename Key, typename SendBufferManager = ygm::locking_send_buffer_manager,
    typename Partitioner = detail::hash_partitioner<Key>,
    typename Compare = std::less<Key>, class Alloc = std::allocator<const Key>,
    int NumBanks = 1024, typename LockBankTag = ygm::DefaultLockBankTag>
class set {
public:
  using self_type = set<Key, SendBufferManager, Partitioner, Compare, Alloc>;
  using key_type = Key;
  using impl_type = detail::set_impl<key_type, SendBufferManager, Partitioner,
                                     Compare, Alloc, NumBanks, LockBankTag>;

  Partitioner partitioner;

  set() = delete;

  set(ygm::comm<SendBufferManager> &comm) : m_impl(comm) {}

  void async_insert(const key_type &key) { m_impl.async_insert_unique(key); }

  void async_erase(const key_type &key) { m_impl.async_erase(key); }

  template <typename Function> void for_all(Function fn) { m_impl.for_all(fn); }

  void clear() { m_impl.clear(); }

  size_t size() { return m_impl.size(); }

  size_t count(const key_type &key) { return m_impl.count(key); }

  void swap(self_type &s) { return m_impl.swap(s.m_impl); }

  void serialize(const std::string &fname) { m_impl.serialize(fname); }
  void deserialize(const std::string &fname) { m_impl.deserialize(fname); }

  typename ygm::ygm_ptr<self_type> get_ygm_ptr() const {
    return m_impl.get_ygm_ptr();
  }

  template <typename Function> void local_for_all(Function fn) {
    m_impl.local_for_all(fn);
  }

  int owner(const key_type &key) const { return m_impl.owner(key); }

private:
  impl_type m_impl;
};

} // namespace ygm::container
