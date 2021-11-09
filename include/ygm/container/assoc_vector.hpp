// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// // Project Developers. See the top-level COPYRIGHT file for details.
// //
// // SPDX-License-Identifier: MIT

#pragma once

#include <ygm/container/detail/assoc_vector_impl.hpp>
namespace ygm::container {

template <typename Key, typename Value,
          typename Partitioner = detail::hash_partitioner<Key>,
          typename Compare     = std::less<Key>,
          class Alloc          = std::allocator<std::pair<const Key, Value>>>
class assoc_vector {
 public:
  using key_type   = Key;
  using value_type = Value;
  using self_type  = assoc_vector<Key, Value, Partitioner, Compare, Alloc>;
  using impl_type  =
      detail::assoc_vector_impl<key_type, value_type, Partitioner, Compare, Alloc>;

  assoc_vector() = delete;

  assoc_vector(ygm::comm& comm) : m_impl(comm) {}

  assoc_vector(ygm::comm& comm, const value_type& dv) : m_impl(comm, dv) {}

  assoc_vector(const self_type& rhs) : m_impl(rhs.m_impl) {}

  ygm::comm& comm() { return m_impl.comm(); }

  template <typename Function>
  void for_all(Function fn) {
    m_impl.for_all(fn);
  }

  void async_insert(const key_type& key, const value_type& value) {
    m_impl.async_insert(key, value);
  }

  template <typename Visitor, typename... VisitorArgs>
  void async_visit_or_insert(const key_type& key, const value_type &value,
                                Visitor visitor, const VisitorArgs&... args) {
    m_impl.async_visit_or_insert(key, value, visitor, std::forward<const VisitorArgs>(args)...);
  }

  void clear() { m_impl.clear(); }

  /* Use this if you want to interact with more that one containers. */
  typename ygm::ygm_ptr<impl_type> get_ygm_ptr() const {
    return m_impl.get_ygm_ptr();
  }

 private:
  impl_type m_impl;
};
} // namespace ygm::container
