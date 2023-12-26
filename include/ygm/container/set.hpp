// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once

#include <cereal/archives/json.hpp>
#include <ygm/container/container_traits.hpp>
#include <ygm/container/detail/hash_partitioner.hpp>
#include <ygm/detail/ygm_ptr.hpp>

namespace ygm::container {

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
  using ptr_type           = typename ygm::ygm_ptr<self_type>;

  Partitioner partitioner;

  set() = delete;

  set(ygm::comm& comm);

  set(const self_type&& rhs) noexcept;

  ~set();

  void async_insert(const key_type& key);

  void async_erase(const key_type& key);

  template <typename Visitor, typename... VisitorArgs>
  void async_insert_exe_if_missing(const key_type& key, Visitor visitor,
                                   const VisitorArgs&... args);

  template <typename Visitor, typename... VisitorArgs>
  void async_insert_exe_if_contains(const key_type& key, Visitor visitor,
                                    const VisitorArgs&... args);

  template <typename Visitor, typename... VisitorArgs>
  void async_exe_if_missing(const key_type& key, Visitor visitor,
                            const VisitorArgs&... args);

  template <typename Visitor, typename... VisitorArgs>
  void async_exe_if_contains(const key_type& key, Visitor visitor,
                             const VisitorArgs&... args);

  template <typename Function>
  void for_all(Function fn);

  template <typename Function>
  void consume_all(Function fn);

  void clear();

  size_type size();

  bool empty() { return size() == 0; }

  size_t count(const key_type& key);

  void swap(self_type& s);

  void serialize(const std::string& fname);
  void deserialize(const std::string& fname);

  ptr_type get_ygm_ptr() const;

  int owner(const key_type& key) const;

  ygm::comm& comm();

 private:
  template <typename Function>
  void local_for_all(Function fn);

  template <typename Function>
  void local_consume_all(Function fn);

 private:
  std::set<key_type, Compare, Alloc> m_local_set;
  ygm::comm&                         m_comm;
  typename ygm::ygm_ptr<self_type>   pthis;
};

template <typename Key, typename Partitioner = detail::hash_partitioner<Key>,
          typename Compare = std::less<Key>,
          class Alloc      = std::allocator<const Key>>
class multiset {
 public:
  using self_type          = multiset<Key, Partitioner, Compare, Alloc>;
  using key_type           = Key;
  using size_type          = size_t;
  using ygm_container_type = ygm::container::set_tag;
  using ygm_for_all_types  = std::tuple<Key>;
  using ptr_type           = typename ygm::ygm_ptr<self_type>;

  Partitioner partitioner;

  multiset() = delete;

  multiset(ygm::comm& comm);

  multiset(const self_type&& rhs) noexcept;

  ~multiset();

  void async_insert(const key_type& key);

  void async_erase(const key_type& key);

  template <typename Visitor, typename... VisitorArgs>
  void async_insert_exe_if_missing(const key_type& key, Visitor visitor,
                                   const VisitorArgs&... args);

  template <typename Visitor, typename... VisitorArgs>
  void async_insert_exe_if_contains(const key_type& key, Visitor visitor,
                                    const VisitorArgs&... args);

  template <typename Visitor, typename... VisitorArgs>
  void async_exe_if_missing(const key_type& key, Visitor visitor,
                            const VisitorArgs&... args);

  template <typename Visitor, typename... VisitorArgs>
  void async_exe_if_contains(const key_type& key, Visitor visitor,
                             const VisitorArgs&... args);

  template <typename Function>
  void for_all(Function fn);

  template <typename Function>
  void consume_all(Function fn);

  void clear();

  size_type size();

  bool empty() { return size() == 0; }

  size_t count(const key_type& key);

  void swap(self_type& s);

  void serialize(const std::string& fname);
  void deserialize(const std::string& fname);

  ptr_type get_ygm_ptr() const;

  int owner(const key_type& key) const;

  ygm::comm& comm();

 private:
  template <typename Function>
  void local_for_all(Function fn);

  template <typename Function>
  void local_consume_all(Function fn);

 private:
  std::multiset<key_type, Compare, Alloc> m_local_set;
  ygm::comm&                              m_comm;
  typename ygm::ygm_ptr<self_type>        pthis;
};
}  // namespace ygm::container

#include <ygm/container/detail/set.ipp>
