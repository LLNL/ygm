// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once
#include <ygm/container/detail/bag_impl.hpp>

namespace ygm::container {
template <typename Item, typename Alloc = std::allocator<Item>>
class bag {
 public:
  using self_type  = bag<Item, Alloc>;
  using value_type = Item;
  using impl_type  = detail::bag_impl<Item, Alloc>;

  bag(ygm::comm &comm) : m_impl(comm) {}

  void async_insert(const value_type &item) { m_impl.async_insert(item); }

  template <typename Function>
  void for_all(Function fn) {
    m_impl.for_all(fn);
  }

  void clear() { m_impl.clear(); }

  size_t size() { return m_impl.size(); }

  void swap(self_type &s) { m_impl.swap(s.m_impl); }

  template <typename RandomFunc>
  void local_shuffle(RandomFunc r) { m_impl.local_shuffle(r); }

  template <typename RandomFunc>
  void global_shuffle(RandomFunc r) { m_impl.global_shuffle(r); }

  template <typename Function>
  void local_for_all(Function fn) {
    m_impl.local_for_all(fn);
  }

  ygm::comm &comm() { return m_impl.comm(); }

  void serialize(const std::string &fname) { m_impl.serialize(fname); }
  void deserialize(const std::string &fname) { m_impl.deserialize(fname); }
  std::vector<value_type> gather_to_vector(int dest) { return m_impl.gather_to_vector(dest); }
  std::vector<value_type> gather_to_vector() { return m_impl.gather_to_vector(); }

 private:
  detail::bag_impl<value_type> m_impl;
};
}  // namespace ygm::container
