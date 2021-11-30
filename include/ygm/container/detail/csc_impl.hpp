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

#include <ygm/container/map.hpp>
#include <ygm/container/assoc_vector.hpp>

#include <ygm/container/detail/adj_impl.hpp>
#include <ygm/container/detail/hash_partitioner.hpp>

//#include <ygm/container/detail/algorithms/spmv.hpp>

namespace ygm::container::detail {

template <typename Key, typename Value,
          typename Partitioner = detail::hash_partitioner<Key>,
          typename Compare     = std::less<Key>,
          class Alloc          = std::allocator<std::pair<const Key, Value>>>
class csc_impl {
 public:
  using key_type    = Key;
  using value_type  = Value;
  using self_type   = csc_impl<Key, Value, Partitioner, Compare, Alloc>;

  using map_type    = ygm::container::map<key_type, value_type>;
  //using map_type    = ygm::container::assoc_vector<key_type, value_type>;
  using adj_impl    = detail::adj_impl<key_type, value_type, Partitioner, Compare, Alloc>;

  Partitioner partitioner;

  csc_impl(ygm::comm &comm) : m_csc(comm), m_comm(comm), pthis(this), m_default_value{} {
    m_comm.barrier();
  }

  csc_impl(ygm::comm &comm, const value_type &dv)
      : m_csc(comm), m_comm(comm), pthis(this), m_default_value(dv) {
    m_comm.barrier();
  }

  csc_impl(const self_type &rhs)
      : m_comm(rhs.m_comm), pthis(this), m_default_value(rhs.m_default_value) {
    m_comm.barrier();
    //m_row_map.insert(std::begin(rhs.m_row_map), std::end(rhs.m_col_map));
    m_comm.barrier();
  }

  ~csc_impl() { m_comm.barrier(); }

  void async_insert(const key_type& row, const key_type& col, const value_type& value) {
    m_csc.async_insert(col, row, value);
  }

  ygm::comm &comm() { return m_comm; }

  /* For all is expected to be const on csc -> ensure 
    * no lambda changes the value of the csc elements... 
    * how do you control parameters? */
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
    m_csc.async_visit_if_exists(col, row, visitor, std::forward<const VisitorArgs>(args)...);
  }

  template <typename Visitor, typename... VisitorArgs>
  void async_visit_col_mutate(const key_type& col, Visitor visitor,
                             const VisitorArgs&... args) {
    /* Accessing the adj map in csc -- 
      * should this actually be enclosed within adj? */
    /*  this means adj should have row and col adj, 
      * and not a single instance inside the csc impl.. */
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
    m_csc.async_visit_const(col, visitor, std::forward<const VisitorArgs>(args)...);
  }

  template <typename Visitor, typename... VisitorArgs>
  void async_visit_or_insert(const key_type& row, const key_type& col, const value_type &value, 
                                Visitor visitor, const VisitorArgs&... args) {
    m_csc.async_visit_or_insert(col, row, value, visitor, std::forward<const VisitorArgs>(args)...);
  }

  #ifdef map_old_def
  map_type spmv(map_type& x) {
    
    auto y = ygm::container::detail::algorithms::spmv(x);
    return y;
  }
  #endif

  #ifdef map_new_defn
  /* This method accepts a map-type object. */
  map_type spmv(map_type& x) {

    map_type y(m_comm);
    auto y_ptr = y.get_ygm_ptr();
    auto A_ptr = pthis->get_ygm_ptr();

    auto kv_lambda = [A_ptr, y_ptr](auto kv_pair) {

      auto &mptrx_comm = A_ptr->comm();
      int rank         = mptrx_comm.rank();
    
      auto col        = kv_pair.first;
      auto col_value  = kv_pair.second;

      auto csc_visit_lambda = [](
        auto col, auto row, 
        auto A_value, auto x_value, auto y_ptr) {

        auto element_wise = A_value * x_value;

        auto append_lambda = [](auto &rv_pair, const auto &update_val) {
          auto row_id = rv_pair.first;
          auto value  = rv_pair.second;
          auto append_val = value + update_val;
          rv_pair.second = rv_pair.second + update_val;
        };

        //y_ptr->async_visit_or_insert(row, element_wise, append_lambda, element_wise);
        y_ptr->async_insert_if_missing_else_visit(row, element_wise, append_lambda);
      }; 
      
      A_ptr->async_visit_col_const(col, csc_visit_lambda, col_value, y_ptr);
    };

    x.for_all(kv_lambda);
    m_comm.barrier();

    return y;
  }
  #endif

  typename ygm::ygm_ptr<self_type> get_ygm_ptr() const { return pthis; }

  void local_clear() { 
    m_csc.clear(); 
  }

 protected:
  csc_impl() = delete;

  value_type                                          m_default_value;
  adj_impl                                            m_csc; 
  ygm::comm                                           m_comm;
  typename ygm::ygm_ptr<self_type>                    pthis;
};
}  // namespace ygm::container::detail
