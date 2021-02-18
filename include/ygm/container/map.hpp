// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once

#include <ygm/container/detail/map_impl.hpp>
namespace ygm::container {

template <typename Key, typename Value,
          typename SendBufferManager = ygm::locking_send_buffer_manager,
          typename Partitioner = detail::hash_partitioner<Key>,
          typename Compare = std::less<Key>,
          class Alloc = std::allocator<std::pair<const Key, Value>>,
          int NumBanks = 1024, typename LockBankTag = ygm::DefaultLockBankTag>
class map {
public:
  using self_type = map<Key, Value, SendBufferManager, Partitioner, Compare,
                        Alloc, NumBanks, LockBankTag>;
  using value_type = Value;
  using key_type = Key;
  using impl_type =
      detail::map_impl<key_type, value_type, SendBufferManager, Partitioner,
                       Compare, Alloc, NumBanks, LockBankTag>;
  map() = delete;

  map(ygm::comm<SendBufferManager> &comm) : m_impl(comm) {}

  map(ygm::comm<SendBufferManager> &comm, const value_type &dv)
      : m_impl(comm, dv) {}

  void async_insert(const std::pair<key_type, value_type> &kv) {
    async_insert(kv.first, kv.second);
  }
  void async_insert(const key_type &key, const value_type &value) {
    m_impl.async_insert_unique(key, value);
  }

  void async_set(const key_type &key, const value_type &value) {
    async_insert(key, value);
  }

  template <typename Visitor, typename... VisitorArgs>
  void async_visit(const key_type &key, Visitor visitor,
                   const VisitorArgs &... args) {
    m_impl.async_visit(key, visitor, std::forward<const VisitorArgs>(args)...);
  }

  template <typename Visitor, typename... VisitorArgs>
  void async_visit_if_exists(const key_type &key, Visitor visitor,
                             const VisitorArgs &... args) {
    m_impl.async_visit_if_exists(key, visitor,
                                 std::forward<const VisitorArgs>(args)...);
  }

  void async_erase(const key_type &key) { m_impl.async_erase(key); }

  size_t local_count(const key_type &key) { return m_impl.local_count(key); }

  template <typename Function> void for_all(Function fn) { m_impl.for_all(fn); }

  void clear() { m_impl.clear(); }

  size_t size() { return m_impl.size(); }

  size_t count(const key_type &key) { return m_impl.count(key); }

  typename ygm::ygm_ptr<impl_type> get_ygm_ptr() const {
    return m_impl.get_ygm_ptr();
  }

  void serialize(const std::string &fname) { m_impl.serialize(fname); }
  void deserialize(const std::string &fname) { m_impl.deserialize(fname); }

  int owner(const key_type &key) const { return m_impl.owner(key); }

  bool is_mine(const key_type &key) const { return m_impl.is_mine(key); }

  std::vector<value_type> local_get(const key_type &key) {
    return m_impl.local_get(key);
  }

  void swap(self_type &s) { m_impl.swap(s.m_impl); }

  template <typename STLKeyContainer>
  std::map<key_type, value_type> all_gather(const STLKeyContainer &keys) {
    std::map<key_type, value_type> to_return;
    m_impl.all_gather(keys, to_return);
    return to_return;
  }

  std::map<key_type, value_type> all_gather(const std::vector<key_type> &keys) {
    std::map<key_type, value_type> to_return;
    m_impl.all_gather(keys, to_return);
    return to_return;
  }

  ygm::comm<SendBufferManager> &comm() { return m_impl.comm(); }

  template <typename CompareFunction>
  std::vector<std::pair<key_type, value_type>> topk(size_t k,
                                                    CompareFunction cfn) {
    return m_impl.topk(k, cfn);
  }

private:
  impl_type m_impl;
};

template <typename Key, typename Value,
          typename SendBufferManager = ygm::locking_send_buffer_manager,
          typename Partitioner = detail::hash_partitioner<Key>,
          typename Compare = std::less<Key>,
          class Alloc = std::allocator<std::pair<const Key, Value>>,
          int NumBanks = 1024, typename LockBankTag = ygm::DefaultLockBankTag>
class multimap {
public:
  using self_type = multimap<Key, Value, SendBufferManager, Partitioner,
                             Compare, Alloc, NumBanks, LockBankTag>;
  using value_type = Value;
  using key_type = Key;
  using impl_type =
      detail::map_impl<key_type, value_type, SendBufferManager, Partitioner,
                       Compare, Alloc, NumBanks, LockBankTag>;
  multimap() = delete;

  multimap(ygm::comm<SendBufferManager> &comm) : m_impl(comm) {}

  multimap(ygm::comm<SendBufferManager> &comm, const value_type &dv)
      : m_impl(comm, dv) {}

  void async_insert(const std::pair<key_type, value_type> &kv) {
    async_insert(kv.first, kv.second);
  }
  void async_insert(const key_type &key, const value_type &value) {
    m_impl.async_insert_multi(key, value);
  }

  template <typename Visitor, typename... VisitorArgs>
  void async_visit(const key_type &key, Visitor visitor,
                   const VisitorArgs &... args) {
    m_impl.async_visit(key, visitor, std::forward<const VisitorArgs>(args)...);
  }

  template <typename Visitor, typename... VisitorArgs>
  void async_visit_if_exists(const key_type &key, Visitor visitor,
                             const VisitorArgs &... args) {
    m_impl.async_visit_if_exists(key, visitor,
                                 std::forward<const VisitorArgs>(args)...);
  }

  void async_erase(const key_type &key) { m_impl.async_erase(key); }

  size_t local_count(const key_type &key) { return m_impl.local_count(key); }

  template <typename Function> void for_all(Function fn) { m_impl.for_all(fn); }

  void clear() { m_impl.clear(); }

  size_t size() { return m_impl.size(); }

  size_t count(const key_type &key) { return m_impl.count(key); }

  typename ygm::ygm_ptr<impl_type> get_ygm_ptr() const {
    return m_impl.get_ygm_ptr();
  }

  void serialize(const std::string &fname) { m_impl.serialize(fname); }
  void deserialize(const std::string &fname) { m_impl.deserialize(fname); }

  int owner(const key_type &key) const { return m_impl.owner(key); }

  bool is_mine(const key_type &key) const { return m_impl.is_mine(key); }

  std::vector<value_type> local_get(const key_type &key) {
    return m_impl.local_get(key);
  }

  void swap(self_type &s) { m_impl.swap(s.m_impl); }

  template <typename STLKeyContainer>
  std::multimap<key_type, value_type> all_gather(const STLKeyContainer &keys) {
    std::multimap<key_type, value_type> to_return;
    m_impl.all_gather(keys, to_return);
    return to_return;
  }

  std::multimap<key_type, value_type>
  all_gather(const std::vector<key_type> &keys) {
    std::multimap<key_type, value_type> to_return;
    m_impl.all_gather(keys, to_return);
    return to_return;
  }

  ygm::comm<SendBufferManager> &comm() { return m_impl.comm(); }

  template <typename CompareFunction>
  std::vector<std::pair<key_type, value_type>> topk(size_t k,
                                                    CompareFunction cfn) {
    return m_impl.topk(k, cfn);
  }

private:
  impl_type m_impl;
};

} // namespace ygm::container
