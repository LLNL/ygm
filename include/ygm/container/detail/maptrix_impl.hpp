// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once
#include <cereal/archives/json.hpp>
#include <cereal/types/utility.hpp>
#include <fstream>
#include <map>
#include <ygm/comm.hpp>
#include <ygm/container/detail/hash_partitioner.hpp>
#include <ygm/detail/ygm_ptr.hpp>
//#include <ygm/container/detail/adj_impl.hpp>
#include <ygm/container/detail/csr_impl.hpp>
#include <ygm/container/detail/csc_impl.hpp>

#include <ygm/container/detail/algorithms/spmv.hpp>

namespace ygm::container::detail {

template <typename Key, typename Value,
          typename Partitioner = detail::hash_partitioner<Key>,
          typename Compare     = std::less<Key>,
          class Alloc          = std::allocator<std::pair<const Key, Value>>>
class maptrix_impl {
 public:
  using key_type   = Key;
  using value_type = Value;
  using self_type  = maptrix_impl<Key, Value, Partitioner, Compare, Alloc>;

  //using map_type   = ygm::container::assoc_vector<key_type, value_type>;
  //using adj_impl   = detail::adj_impl<key_type, value_type, Partitioner, Compare, Alloc>;

  using csr_impl   = detail::csr_impl<key_type, value_type, Partitioner, Compare, Alloc>;
  using csc_impl   = detail::csc_impl<key_type, value_type, Partitioner, Compare, Alloc>;
  using map_type   = ygm::container::map<key_type, value_type>;

  Partitioner partitioner;

  maptrix_impl(ygm::comm &comm) : m_csr(comm), m_csc(comm), m_comm(comm), pthis(this), m_default_value{} {
    m_comm.barrier();
  }

  maptrix_impl(ygm::comm &comm, const value_type &dv)
      : m_csr(comm), m_csc(comm), m_comm(comm), pthis(this), m_default_value(dv) {
    m_comm.barrier();
  }

  maptrix_impl(const self_type &rhs)
      : m_comm(rhs.m_comm), pthis(this), m_default_value(rhs.m_default_value) {
    m_comm.barrier();
    //m_row_map.insert(std::begin(rhs.m_row_map), std::end(rhs.m_col_map));
    m_comm.barrier();
  }

  ~maptrix_impl() { m_comm.barrier(); }

  void async_insert(const key_type& row, const key_type& col, const value_type& value) {
    m_csr.async_insert(row, col, value);
    m_csc.async_insert(row, col, value);
  }

  //*** TBD: Should you support this function? ***//
  ygm::comm &comm() { return m_comm; }

  /* For all is expected to be const on csc. */
  template <typename Function>
  void for_all(Function fn) {
    m_csc.for_all(fn);
  }

  template <typename... VisitorArgs>
  void print_all(std::ostream& os, VisitorArgs const&... args) {
    ((os << args), ...);
  }

  template <typename Visitor, typename... VisitorArgs>
  void async_visit_if_exists(const key_type &row, const key_type &col, 
          Visitor visitor, const VisitorArgs &...args) {
    m_csr.async_visit_if_exists(row, col, visitor, std::forward<const VisitorArgs>(args)...);
    m_csc.async_visit_if_exists(row, col, visitor, std::forward<const VisitorArgs>(args)...);
  }

  template <typename Visitor, typename... VisitorArgs>
  void async_visit_col_mutate(const key_type& col, Visitor visitor,
                             const VisitorArgs&... args) {
    /* Accessing the adj map in maptrix -- 
      * should this actually be enclosed within adj? */
    /*  this means adj should have row and col adj, 
      * and not a single instance inside the maptrix impl.. */
    auto &m_map     = m_csc.adj();
    auto &inner_map = m_map.find(col)->second;
    for (auto itr = inner_map.begin(); itr != inner_map.end(); ++itr) {
      key_type row  = itr->first;
      pthis->async_visit_if_exists(row, col, 
                  visitor, std::forward<const VisitorArgs>(args)...);
    }    
  }

  template <typename Visitor, typename... VisitorArgs>
  void async_visit_col_const(const key_type &col, Visitor visitor,
                             const VisitorArgs &...args) {
    m_csc.async_visit_col_const(col, visitor, std::forward<const VisitorArgs>(args)...);
  }

  template <typename Visitor, typename... VisitorArgs>
  void async_visit_or_insert(const key_type& row, const key_type& col, const value_type &value, 
                                Visitor visitor, const VisitorArgs&... args) {
    m_csc.async_visit_or_insert(row, col, value, visitor, std::forward<const VisitorArgs>(args)...);
  }

  map_type spmv(map_type& x) {
    //auto y = ygm::container::detail::algorithms::spmv(this, x);
    auto y = ygm::container::detail::algorithms::spmv(pthis, x);
    //auto y = spmv(pthis, x);
    return y;
  }

  typename ygm::ygm_ptr<self_type> get_ygm_ptr() const { return pthis; }

  void local_clear() { 
    m_csr.clear(); 
    m_csc.clear(); 
  }


 protected:
  maptrix_impl() = delete;

  value_type                                          m_default_value;
  csr_impl                                            m_csr;      
  csc_impl                                            m_csc; 
  ygm::comm                                           m_comm;
  typename ygm::ygm_ptr<self_type>                    pthis;
};
}  // namespace ygm::container::detail
