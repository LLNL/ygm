// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once

#include <map>
#include <fstream>
#include <ygm/comm.hpp>
#include <ygm/detail/ygm_ptr.hpp>
#include <cereal/archives/json.hpp>
#include <cereal/types/utility.hpp>

#include <ygm/container/experimental/detail/adj_impl.hpp>
#include <ygm/container/detail/hash_partitioner.hpp>

namespace ygm::container::experimental::detail {

template <typename Key, typename Value,
          typename Partitioner = ygm::container::detail::hash_partitioner<Key>,
          typename Compare     = std::less<Key>,
          class Alloc          = std::allocator<std::pair<const Key, Value>>>
class csr_impl {
 public:
  using key_type    = Key;
  using value_type  = Value;
  using self_type   = csr_impl<Key, Value, Partitioner, Compare, Alloc>;
  using adj_impl    = detail::adj_impl<key_type, value_type, Partitioner, Compare, Alloc>;
  using inner_map_type  = std::map<Key, Value>;

  Partitioner partitioner;

  csr_impl(ygm::comm &comm) : m_csr(comm), m_comm(comm), pthis(this), m_default_value{} {
    m_comm.barrier();
  }

  csr_impl(ygm::comm &comm, const value_type &dv)
      : m_csr(comm), m_comm(comm), pthis(this), m_default_value(dv) {
    m_comm.barrier();
  }

  ~csr_impl() { m_comm.barrier(); }

  void async_insert(const key_type &row, const key_type &col, const value_type &value) {
    m_csr.async_insert(row, col, value);
  }

  ygm::comm &comm() { return m_comm; }

  template <typename Function>
  void for_all(Function fn) {
    m_csr.for_all(fn);
  }

  template <typename Function>
  void for_all_row(Function fn) {
    m_csr.for_all_outer_key(fn);
  }

  template <typename... VisitorArgs>
  void print_all(std::ostream& os, VisitorArgs const&... args) {
    ((os << args), ...);
  }

  template <typename Visitor, typename... VisitorArgs>
  void async_visit_if_exists(const key_type &row, const key_type &col, 
          Visitor visitor, const VisitorArgs &...args) {
    m_csr.async_visit_if_exists(row, col, visitor, std::forward<const VisitorArgs>(args)...);
  }

  template <typename Visitor, typename... VisitorArgs>
  void async_insert_if_missing_else_visit(const key_type &row, const key_type &col, const value_type &value, 
                                Visitor visitor, const VisitorArgs&... args) {
    m_csr.async_insert_if_missing_else_visit(row, col, value, visitor, std::forward<const VisitorArgs>(args)...);
  }

  template <typename Visitor, typename... VisitorArgs>
  void async_visit_row_const(const key_type &row, Visitor visitor,
                             const VisitorArgs &...args) {
    m_csr.async_visit_const(row, visitor, std::forward<const VisitorArgs>(args)...);
  }

  typename ygm::ygm_ptr<self_type> get_ygm_ptr() const { return pthis; }

  void local_clear() { 
    m_csr.clear(); 
  }

  void swap(self_type &s) {
    m_csr.swap(s);
  }

 protected:
  csr_impl() = delete;

  value_type                                          m_default_value;

  adj_impl                                            m_csr;      
  ygm::comm                                           m_comm;
  typename ygm::ygm_ptr<self_type>                    pthis;
};
}  // namespace ygm::container::experimental::detail
