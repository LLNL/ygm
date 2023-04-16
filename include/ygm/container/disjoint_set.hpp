// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once
#include <ygm/container/detail/disjoint_set_impl.hpp>

namespace ygm::container {
template <typename Item, typename Partitioner = detail::hash_partitioner<Item>>
class disjoint_set {
 public:
  using self_type  = disjoint_set<Item, Partitioner>;
  using value_type = Item;
  using impl_type  = detail::disjoint_set_impl<Item, Partitioner>;

  disjoint_set() = delete;

  disjoint_set(ygm::comm &comm) : m_impl(comm) {}

  void async_union(const value_type &a, const value_type &b) {
    m_impl.async_union(a, b);
  }

  template <typename Function, typename... FunctionArgs>
  void async_union_and_execute(const value_type &a, const value_type &b,
                               Function fn, const FunctionArgs &...args) {
    m_impl.async_union_and_execute(a, b, fn,
                                   std::forward<const FunctionArgs>(args)...);
  }

  void all_compress() { m_impl.all_compress(); }

  template <typename Function>
  void for_all(Function fn) {
    m_impl.for_all(fn);
  }

  std::map<value_type, value_type> all_find(
      const std::vector<value_type> &items) {
    return m_impl.all_find(items);
  }

  size_t size() { return m_impl.size(); }

  size_t num_sets() { return m_impl.num_sets(); }

  typename ygm::ygm_ptr<impl_type> get_ygm_ptr() const {
    return m_impl.get_ygm_ptr();
  }

 private:
  impl_type m_impl;
};
}  // namespace ygm::container
