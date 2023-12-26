// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once

#include <map>

#include <ygm/container/container_traits.hpp>
#include <ygm/container/detail/hash_partitioner.hpp>
#include <ygm/detail/ygm_ptr.hpp>

namespace ygm::container {

template <typename Key, typename Value,
          typename Partitioner = detail::hash_partitioner<Key>,
          typename Compare     = std::less<Key>,
          class Alloc          = std::allocator<std::pair<const Key, Value>>>
class map {
 public:
  using self_type          = map<Key, Value, Partitioner, Compare, Alloc>;
  using mapped_type        = Value;
  using key_type           = Key;
  using size_type          = size_t;
  using ygm_for_all_types  = std::tuple<Key, Value>;
  using ygm_container_type = ygm::container::map_tag;
  using ptr_type           = typename ygm::ygm_ptr<self_type>;

  Partitioner partitioner;

  map() = delete;

  map(ygm::comm& comm);

  map(ygm::comm& comm, const mapped_type& dv);

  map(const self_type& rhs);

  ~map();

  void async_insert(const std::pair<key_type, mapped_type>& kv);
  void async_insert(const key_type& key, const mapped_type& value);

  void async_insert_if_missing(const std::pair<key_type, mapped_type>& kv);
  void async_insert_if_missing(const key_type& key, const mapped_type& value);

  void async_set(const key_type& key, const mapped_type& value);

  template <typename Visitor, typename... VisitorArgs>
  void async_visit(const key_type& key, Visitor visitor,
                   const VisitorArgs&... args);

  template <typename Visitor, typename... VisitorArgs>
  void async_visit_if_exists(const key_type& key, Visitor visitor,
                             const VisitorArgs&... args);

  template <typename Visitor, typename... VisitorArgs>
  void async_insert_if_missing_else_visit(const key_type&    key,
                                          const mapped_type& value,
                                          Visitor            visitor,
                                          const VisitorArgs&... args);

  template <typename ReductionOp>
  void async_reduce(const key_type& key, const mapped_type& value,
                    ReductionOp reducer);

  void async_erase(const key_type& key);

  size_t local_count(const key_type& key);

  template <typename Function>
  void for_all(Function fn);

  void clear();

  size_type size();

  size_t count(const key_type& key);

  ptr_type get_ygm_ptr() const;

  void serialize(const std::string& fname);
  void deserialize(const std::string& fname);

  int owner(const key_type& key) const;

  bool is_mine(const key_type& key) const;

  std::vector<mapped_type> local_get(const key_type& key);

  void swap(self_type& s);

  template <typename STLKeyContainer>
  std::map<key_type, mapped_type> all_gather(const STLKeyContainer& keys);

  ygm::comm& comm();

  template <typename CompareFunction>
  std::vector<std::pair<key_type, mapped_type>> topk(size_t          k,
                                                     CompareFunction cfn);

  const mapped_type& default_value() const;

 private:
  template <typename Function, typename... VisitorArgs>
  void local_visit(const key_type& key, Function& fn,
                   const VisitorArgs&... args);

  void local_erase(const key_type& key);

  template <typename Function>
  void local_for_all(Function fn);

 private:
  mapped_type                                     m_default_value;
  std::map<key_type, mapped_type, Compare, Alloc> m_local_map;
  ygm::comm&                                      m_comm;
  ptr_type                                        pthis;
};

template <typename Key, typename Value,
          typename Partitioner = detail::hash_partitioner<Key>,
          typename Compare     = std::less<Key>,
          class Alloc          = std::allocator<std::pair<const Key, Value>>>
class multimap {
 public:
  using self_type          = multimap<Key, Value, Partitioner, Compare, Alloc>;
  using mapped_type        = Value;
  using key_type           = Key;
  using size_type          = size_t;
  using ygm_for_all_types  = std::tuple<Key, Value>;
  using ygm_container_type = ygm::container::map_tag;
  using ptr_type           = typename ygm::ygm_ptr<self_type>;

  Partitioner partitioner;

  multimap() = delete;

  multimap(ygm::comm& comm);

  multimap(ygm::comm& comm, const mapped_type& dv);

  multimap(const self_type& rhs);

  ~multimap();

  void async_insert(const std::pair<key_type, mapped_type>& kv);
  void async_insert(const key_type& key, const mapped_type& value);

  void async_insert_if_missing(const std::pair<key_type, mapped_type>& kv);
  void async_insert_if_missing(const key_type& key, const mapped_type& value);

  void async_set(const key_type& key, const mapped_type& value);

  template <typename Visitor, typename... VisitorArgs>
  void async_visit(const key_type& key, Visitor visitor,
                   const VisitorArgs&... args);

  template <typename Visitor, typename... VisitorArgs>
  void async_visit_group(const key_type& key, Visitor visitor,
                         const VisitorArgs&... args);

  template <typename Visitor, typename... VisitorArgs>
  void async_visit_if_exists(const key_type& key, Visitor visitor,
                             const VisitorArgs&... args);

  template <typename Visitor, typename... VisitorArgs>
  void async_insert_if_missing_else_visit(const key_type&    key,
                                          const mapped_type& value,
                                          Visitor            visitor,
                                          const VisitorArgs&... args);

  template <typename ReductionOp>
  void async_reduce(const key_type& key, const mapped_type& value,
                    ReductionOp reducer);

  void async_erase(const key_type& key);

  size_t local_count(const key_type& key);

  template <typename Function>
  void for_all(Function fn);

  void clear();

  size_type size();

  size_t count(const key_type& key);

  ptr_type get_ygm_ptr() const;

  void serialize(const std::string& fname);
  void deserialize(const std::string& fname);

  int owner(const key_type& key) const;

  bool is_mine(const key_type& key) const;

  std::vector<mapped_type> local_get(const key_type& key);

  void swap(self_type& s);

  template <typename STLKeyContainer>
  std::multimap<key_type, mapped_type> all_gather(const STLKeyContainer& keys);

  ygm::comm& comm();

  template <typename CompareFunction>
  std::vector<std::pair<key_type, mapped_type>> topk(size_t          k,
                                                     CompareFunction cfn);

  const mapped_type& default_value() const;

 private:
  template <typename Function, typename... VisitorArgs>
  void local_visit(const key_type& key, Function& fn,
                   const VisitorArgs&... args);

  void local_erase(const key_type& key);

  template <typename Function>
  void local_for_all(Function fn);

 private:
  mapped_type                                          m_default_value;
  std::multimap<key_type, mapped_type, Compare, Alloc> m_local_map;
  ygm::comm&                                           m_comm;
  ptr_type                                             pthis;
};

}  // namespace ygm::container

#include <ygm/container/detail/map.ipp>
