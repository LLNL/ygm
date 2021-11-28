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
#include <ygm/container/assoc_vector.hpp>
#include <ygm/container/detail/adj_impl.hpp>
#include <ygm/container/detail/hash_partitioner.hpp>

namespace ygm::container::detail {

template <typename Key, typename Value,
          typename Partitioner = detail::hash_partitioner<Key>,
          typename Compare     = std::less<Key>,
          class Alloc          = std::allocator<std::pair<const Key, Value>>>
class csr_impl {
 public:
  using key_type    = Key;
  using value_type  = Value;
  using self_type   = csr_impl<Key, Value, Partitioner, Compare, Alloc>;
  using map_type    = ygm::container::assoc_vector<key_type, value_type>;
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

  csr_impl(const self_type &rhs)
      : m_comm(rhs.m_comm), pthis(this), m_default_value(rhs.m_default_value) {
    m_comm.barrier();
    //m_row_map.insert(std::begin(rhs.m_row_map), std::end(rhs.m_col_map));
    m_comm.barrier();
  }

  ~csr_impl() { m_comm.barrier(); }

  void async_insert(const key_type& row, const key_type& col, const value_type& value) {
    m_csr.async_insert(row, col, value);
  }

  ygm::comm &comm() { return m_comm; }

  template <typename Function>
  void for_all(Function fn) {
    m_csr.for_all(fn);
  }

  template <typename... VisitorArgs>
  void print_all(std::ostream& os, VisitorArgs const&... args) {
    ((os << args), ...);
  }

  template <typename Visitor, typename... VisitorArgs>
  void async_visit_if_exists(const key_type &row, const key_type &col, 
          Visitor visitor, const VisitorArgs &...args) {
    m_csr.async_visit_if_exists(col, row, visitor, std::forward<const VisitorArgs>(args)...);
  }

  template <typename Visitor, typename... VisitorArgs>
  void async_visit_or_insert(const key_type& row, const key_type& col, const value_type &value, 
                                Visitor visitor, const VisitorArgs&... args) {
    m_csr.async_visit_or_insert(row, col, value, visitor, std::forward<const VisitorArgs>(args)...);
  }

  typename ygm::ygm_ptr<self_type> get_ygm_ptr() const { return pthis; }

  void local_clear() { 
    m_csr.clear(); 
  }

 protected:
  csr_impl() = delete;

  value_type                                          m_default_value;

  adj_impl                                            m_csr;      
  ygm::comm                                           m_comm;
  typename ygm::ygm_ptr<self_type>                    pthis;
};
}  // namespace ygm::container::detail
