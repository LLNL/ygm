// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once

#include <ygm/container/container_traits.hpp>
#include <ygm/container/detail/set_impl.hpp>

namespace ygm::container {

template <typename Key, typename Partitioner = detail::hash_partitioner<Key>,
          typename Compare = std::less<Key>,
          class Alloc      = std::allocator<const Key>>
class multiset {
 public:
  using self_type         = multiset<Key, Partitioner, Compare, Alloc>;
  using key_type          = Key;
  using size_type         = size_t;
  using ygm_for_all_types = std::tuple<Key>;
  using impl_type = detail::set_impl<key_type, Partitioner, Compare, Alloc>;

  Partitioner partitioner;

  multiset() = delete;

  multiset(ygm::comm& comm) : m_impl(comm) {}

  void async_insert(const key_type& key) { m_impl.async_insert_multi(key); }

  void async_erase(const key_type& key) { m_impl.async_erase(key); }

  template <typename Function>
  void for_all(Function fn) {
    m_impl.for_all(fn);
  }

  template <typename Function>
  void consume_all(Function fn) {
    m_impl.consume_all(fn);
  }

  void clear() { m_impl.clear(); }

  size_type size() { return m_impl.size(); }

  bool empty() { return m_impl.size() == 0; }

  size_t count(const key_type& key) { return m_impl.count(key); }

  void swap(self_type& s) { return m_impl.swap(s.m_impl); }

  void serialize(const std::string& fname) { m_impl.serialize(fname); }
  void deserialize(const std::string& fname) { m_impl.deserialize(fname); }

  typename ygm::ygm_ptr<impl_type> get_ygm_ptr() const {
    return m_impl.get_ygm_ptr();
  }

  template <typename Function>
  void local_for_all(Function fn) {
    m_impl.local_for_all(fn);
  }

  int owner(const key_type& key) const { return m_impl.owner(key); }

  ygm::comm& comm() { return m_impl.comm(); }

 private:
  impl_type m_impl;
};

template <typename Key, typename Partitioner = detail::hash_partitioner<Key>,
          typename Compare = std::less<Key>,
          class Alloc      = std::allocator<const Key>>
class set {
 public:
  using self_type          = set<Key, Partitioner, Compare, Alloc>;
  using key_type           = Key;
  using size_type          = size_t;
  using ygm_container_type = ygm::container::set_tag;
  using ygm_for_all_types  = std::tuple<Key>;
  using impl_type = detail::set_impl<key_type, Partitioner, Compare, Alloc>;

  Partitioner partitioner;

  set() = delete;

  set(ygm::comm& comm) : m_impl(comm) {}

  void async_insert(const key_type& key) { m_impl.async_insert_unique(key); }

  void async_erase(const key_type& key) { m_impl.async_erase(key); }

  template <typename Visitor, typename... VisitorArgs>
  void async_insert_exe_if_missing(const key_type& key, Visitor visitor,
                                   const VisitorArgs&... args) {
    m_impl.async_insert_exe_if_missing(
        key, visitor, std::forward<const VisitorArgs>(args)...);
  }

  template <typename Visitor, typename... VisitorArgs>
  void async_insert_exe_if_contains(const key_type& key, Visitor visitor,
                                    const VisitorArgs&... args) {
    m_impl.async_insert_exe_if_contains(
        key, visitor, std::forward<const VisitorArgs>(args)...);
  }

  template <typename Visitor, typename... VisitorArgs>
  void async_exe_if_missing(const key_type& key, Visitor visitor,
                            const VisitorArgs&... args) {
    m_impl.async_exe_if_missing(key, visitor,
                                std::forward<const VisitorArgs>(args)...);
  }

  template <typename Visitor, typename... VisitorArgs>
  void async_exe_if_contains(const key_type& key, Visitor visitor,
                             const VisitorArgs&... args) {
    m_impl.async_exe_if_contains(key, visitor,
                                 std::forward<const VisitorArgs>(args)...);
  }

  template <typename Function>
  void for_all(Function fn) {
    m_impl.for_all(fn);
  }

  template <typename Function>
  void consume_all(Function fn) {
    m_impl.consume_all(fn);
  }

  void clear() { m_impl.clear(); }

  size_type size() { return m_impl.size(); }

  bool empty() { return m_impl.size() == 0; }

  size_t count(const key_type& key) { return m_impl.count(key); }

  void swap(self_type& s) { return m_impl.swap(s.m_impl); }

  void serialize(const std::string& fname) { m_impl.serialize(fname); }
  void deserialize(const std::string& fname) { m_impl.deserialize(fname); }

  typename ygm::ygm_ptr<impl_type> get_ygm_ptr() const {
    return m_impl.get_ygm_ptr();
  }

  template <typename Function>
  void local_for_all(Function fn) {
    m_impl.local_for_all(fn);
  }

  int owner(const key_type& key) const { return m_impl.owner(key); }

  ygm::comm& comm() { return m_impl.comm(); }

 private:
  impl_type m_impl;
};

}  // namespace ygm::container
