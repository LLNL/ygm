// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once

#include <ygm/container/detail/map_impl.hpp>
#include <ygm/container/container_traits.hpp>

namespace ygm::container {

template <typename Key, typename Value,
          typename Partitioner = detail::hash_partitioner<Key>,
          typename Compare     = std::less<Key>,
          class Alloc          = std::allocator<std::pair<const Key, Value>>>
class map {
 public:
  using self_type           = map<Key, Value, Partitioner, Compare, Alloc>;
  using mapped_type         = Value;
  using key_type            = Key;
  using size_type           = size_t;
  using ygm_for_all_types   = std::tuple< Key, Value >;
  using ygm_container_type  = ygm::container::map_tag;
  using impl_type =
      detail::map_impl<key_type, mapped_type, Partitioner, Compare, Alloc>;

  map() = delete;

  map(ygm::comm& comm) : m_impl(comm) {}

  map(ygm::comm& comm, const mapped_type& dv) : m_impl(comm, dv) {}

  map(const self_type& rhs) : m_impl(rhs.m_impl) {}

  void async_insert(const std::pair<key_type, mapped_type>& kv) {
    async_insert(kv.first, kv.second);
  }
  void async_insert(const key_type& key, const mapped_type& value) {
    m_impl.async_insert_unique(key, value);
  }

  void async_insert_if_missing(const std::pair<key_type, mapped_type>& kv) {
    async_insert_if_missing(kv.first, kv.second);
  }
  void async_insert_if_missing(const key_type& key, const mapped_type& value) {
    m_impl.async_insert_if_missing(key, value);
  }

  void async_set(const key_type& key, const mapped_type& value) {
    async_insert(key, value);
  }

  template <typename Visitor, typename... VisitorArgs>
  void async_visit(const key_type& key, Visitor visitor,
                   const VisitorArgs&... args) {
    m_impl.async_visit(key, visitor, std::forward<const VisitorArgs>(args)...);
  }

  template <typename Visitor, typename... VisitorArgs>
  void async_visit_if_exists(const key_type& key, Visitor visitor,
                             const VisitorArgs&... args) {
    m_impl.async_visit_if_exists(key, visitor,
                                 std::forward<const VisitorArgs>(args)...);
  }

  template <typename Visitor, typename... VisitorArgs>
  void async_insert_if_missing_else_visit(const key_type&   key,
                                          const mapped_type& value,
                                          Visitor           visitor,
                                          const VisitorArgs&... args) {
    m_impl.async_insert_if_missing_else_visit(
        key, value, visitor, std::forward<const VisitorArgs>(args)...);
  }

  template <typename ReductionOp>
  void async_reduce(const key_type& key, const mapped_type& value,
                    ReductionOp reducer) {
    m_impl.async_reduce(key, value, reducer);
  }

  void async_erase(const key_type& key) { m_impl.async_erase(key); }

  size_t local_count(const key_type& key) { return m_impl.local_count(key); }

  template <typename Function>
  void for_all(Function fn) {
    m_impl.for_all(fn);
  }

  void clear() { m_impl.clear(); }

  size_type size() { return m_impl.size(); }

  size_t count(const key_type& key) { return m_impl.count(key); }

  typename ygm::ygm_ptr<impl_type> get_ygm_ptr() const {
    return m_impl.get_ygm_ptr();
  }

  void serialize(const std::string& fname) { m_impl.serialize(fname); }
  void deserialize(const std::string& fname) { m_impl.deserialize(fname); }

  int owner(const key_type& key) const { return m_impl.owner(key); }

  bool is_mine(const key_type& key) const { return m_impl.is_mine(key); }

  std::vector<mapped_type> local_get(const key_type& key) {
    return m_impl.local_get(key);
  }

  void swap(self_type& s) { m_impl.swap(s.m_impl); }

  template <typename STLKeyContainer>
  std::map<key_type, mapped_type> all_gather(const STLKeyContainer& keys) {
    std::map<key_type, mapped_type> to_return;
    m_impl.all_gather(keys, to_return);
    return to_return;
  }

  std::map<key_type, mapped_type> all_gather(const std::vector<key_type>& keys) {
    std::map<key_type, mapped_type> to_return;
    m_impl.all_gather(keys, to_return);
    return to_return;
  }

  ygm::comm& comm() { return m_impl.comm(); }

  template <typename CompareFunction>
  std::vector<std::pair<key_type, mapped_type>> topk(size_t          k,
                                                    CompareFunction cfn) {
    return m_impl.topk(k, cfn);
  }

  const mapped_type& default_value() const { return m_impl.default_value(); }

 private:
  impl_type m_impl;
};

template <typename Key, typename Value,
          typename Partitioner = detail::hash_partitioner<Key>,
          typename Compare     = std::less<Key>,
          class Alloc          = std::allocator<std::pair<const Key, Value>>>
class multimap {
 public:
  using self_type     = multimap<Key, Value, Partitioner, Compare, Alloc>;
  using mapped_type   = Value;
  using key_type      = Key;
  using size_type     = size_t;
  using impl_type =
      detail::map_impl<key_type, mapped_type, Partitioner, Compare, Alloc>;
  multimap() = delete;

  multimap(ygm::comm& comm) : m_impl(comm) {}

  multimap(ygm::comm& comm, const mapped_type& dv) : m_impl(comm, dv) {}

  multimap(const self_type& rhs) : m_impl(rhs.m_impl) {}

  void async_insert(const std::pair<key_type, mapped_type>& kv) {
    async_insert(kv.first, kv.second);
  }
  void async_insert(const key_type& key, const mapped_type& value) {
    m_impl.async_insert_multi(key, value);
  }

  template <typename Visitor, typename... VisitorArgs>
  void async_visit(const key_type& key, Visitor visitor,
                   const VisitorArgs&... args) {
    m_impl.async_visit(key, visitor, std::forward<const VisitorArgs>(args)...);
  }

  template <typename Visitor, typename... VisitorArgs>
  void async_visit_group(const key_type& key, Visitor visitor,
                         const VisitorArgs&... args) {
    m_impl.async_visit_group(key, visitor,
                             std::forward<const VisitorArgs>(args)...);
  }

  template <typename Visitor, typename... VisitorArgs>
  void async_visit_if_exists(const key_type& key, Visitor visitor,
                             const VisitorArgs&... args) {
    m_impl.async_visit_if_exists(key, visitor,
                                 std::forward<const VisitorArgs>(args)...);
  }

  void async_erase(const key_type& key) { m_impl.async_erase(key); }

  size_t local_count(const key_type& key) { return m_impl.local_count(key); }

  template <typename Function>
  void for_all(Function fn) {
    m_impl.for_all(fn);
  }

  void clear() { m_impl.clear(); }

  size_type size() { return m_impl.size(); }

  size_t count(const key_type& key) { return m_impl.count(key); }

  typename ygm::ygm_ptr<impl_type> get_ygm_ptr() const {
    return m_impl.get_ygm_ptr();
  }

  void serialize(const std::string& fname) { m_impl.serialize(fname); }
  void deserialize(const std::string& fname) { m_impl.deserialize(fname); }

  int owner(const key_type& key) const { return m_impl.owner(key); }

  bool is_mine(const key_type& key) const { return m_impl.is_mine(key); }

  std::vector<mapped_type> local_get(const key_type& key) {
    return m_impl.local_get(key);
  }

  void swap(self_type& s) { m_impl.swap(s.m_impl); }

  template <typename STLKeyContainer>
  std::multimap<key_type, mapped_type> all_gather(const STLKeyContainer& keys) {
    std::multimap<key_type, mapped_type> to_return;
    m_impl.all_gather(keys, to_return);
    return to_return;
  }

  std::multimap<key_type, mapped_type> all_gather(
      const std::vector<key_type>& keys) {
    std::multimap<key_type, mapped_type> to_return;
    m_impl.all_gather(keys, to_return);
    return to_return;
  }

  ygm::comm& comm() { return m_impl.comm(); }

  template <typename CompareFunction>
  std::vector<std::pair<key_type, mapped_type>> topk(size_t          k,
                                                    CompareFunction cfn) {
    return m_impl.topk(k, cfn);
  }

  const mapped_type& default_value() const { return m_impl.default_value(); }

 private:
  impl_type m_impl;
};

}  // namespace ygm::container
