// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once

#include <ygm/container/detail/maptrix_impl.hpp>
namespace ygm::container {

/* Find out if you need to change the 
 * name of the row/col variables: key_i, key_j? */
template <typename Key, typename Value, 
          typename Partitioner = detail::hash_partitioner<Key>, 
          typename Compare     = std::less<Key>,
          class Alloc          = std::allocator<std::pair<const Key, Value>>>
class maptrix {
 public:
  using key_type   = Key;
  using value_type = Value;
  using self_type  = maptrix<Key, Value, Partitioner, Compare, Alloc>;
  using impl_type  =
      detail::maptrix_impl<key_type, value_type, Partitioner, Compare, Alloc>;
  maptrix() = delete;

  maptrix(ygm::comm& comm) : m_impl(comm) {}

  maptrix(ygm::comm& comm, const value_type& dv) : m_impl(comm, dv) {}

  maptrix(const self_type& rhs) : m_impl(rhs.m_impl) {}

  /* Should is_symmetric flag be included here? -- Yes. */
  void async_insert(const key_type& row, const key_type& col, const value_type& value) {
    /* Define this function in the maptrix impl. */
    m_impl.async_insert(row, col, value);
  }

  /* Ideally implemented as a message which will then generate another message. */
  bool is_mine(const key_type& row, const key_type& col) const { return m_impl.is_mine(row, col); }

  int owner(const key_type& row, const key_type& col) const { return m_impl.owner(row, col); }

  template <typename Function>
  void for_all(Function fn) {
    m_impl.for_all(fn);
  }

  ygm::comm& comm() { return m_impl.comm(); }

  template <typename Visitor, typename... VisitorArgs>
  void async_visit_if_exists(const key_type& row, const key_type& col, Visitor visitor,
                             const VisitorArgs&... args) {
    m_impl.async_visit_if_exists(row, col, visitor,
                                 std::forward<const VisitorArgs>(args)...);
  }

  template <typename Visitor, typename... VisitorArgs>
  void async_visit_or_insert(const key_type& row, const key_type& col, 
                             const value_type &value, Visitor visitor, 
                             const VisitorArgs&... args) {
    m_impl.async_visit_or_update(row, col, value, visitor, 
                                 std::forward<const VisitorArgs>(args)...); 
  }

  /* Expect the row-data and col-data of an identifier to be 
    * placed in the same node. */
  template <typename Visitor, typename... VisitorArgs>
  void async_visit_col_if_exists(const key_type& col, Visitor visitor,
                             const VisitorArgs&... args) {
    m_impl.async_visit_col_if_exists(col, visitor, std::forward<const VisitorArgs>(args)...);
  }

  /*****************************************************************************************/
  /*****************************************************************************************/
  #ifdef api_creation
  template <typename Visitor, typename... VisitorArgs>
  void async_visit_row_if_exists(const key_type& row, Visitor visitor,
                             const VisitorArgs&... args) {
    m_impl.async_visit_row_if_exists(row, visitor, std::forward<const VisitorArgs>(args)...);
  }

  void async_erase(const key_type& row, const key_type& col) { m_impl.async_erase(row, col); }

  size_t local_count(const key_type& row, const key_type& col) { return m_impl.local_count(row, col); }

  void clear() { m_impl.clear(); }

  size_t size() { return m_impl.size(); }

  size_t count_row(const key_type& row) { return m_impl.count_row(row); }

  size_t count_col(const key_type& col) { return m_impl.count_col(col); }

  /* Use this if you want to interact with more that one containers. */
  typename ygm::ygm_ptr<impl_type> get_ygm_ptr() const {
    return m_impl.get_ygm_ptr();
  }

  void serialize(const std::string& fname) { m_impl.serialize(fname); }
  void deserialize(const std::string& fname) { m_impl.deserialize(fname); }

  /* The same node is expected for owning the row content  
    * and col content of a given identifier. All metadata 
    * entries in the row and the col of that id is expected 
    * to be in the same node. */
  /* Make sure that it performs well - either row/col works. */
  /* Does any instance of this row-id exist in this machine? */
  bool is_mine(const key_type& row) const { return m_impl.is_mine(row); }
  /* Should is_mine check for transpose as well? */
  bool is_mine(const key_type& col) const { return m_impl.is_mine(col); }

  /* Should local_get get row/col? Not supporting multi-map right now.. */
  value_type local_get(const key_type& row, const key_type& col) {
    return m_impl.local_get(row, col);
  }

  /* Impl local_get should also support with row/col, ie., just row or, just col? */
  ygm::container::map<key_type, value_type> local_get(const key_type& row) {
    return m_impl.local_get(row);
  }

  ygm::container::map<key_type, value_type> local_get(const key_type& col) {
    return m_impl.local_get(col);
  }

  void swap(self_type& s) { m_impl.swap(s.m_impl); }

  template <typename STLKeyContainer>
  std::map<key_type, value_type> all_gather(const STLKeyContainer& keys) {
    std::map<key_type, value_type> to_return;
    /* This is defined in the impl file. */
    m_impl.all_gather(keys, to_return);
    return to_return;
  }

  std::map<key_type, value_type> all_gather(const std::vector<key_type>& keys) {
    std::map<key_type, value_type> to_return;
    m_impl.all_gather(keys, to_return);
    return to_return;
  }

  template <typename CompareFunction>
  std::vector<std::pair<key_type, value_type>> topk(size_t          k,
                                                    CompareFunction cfn) {
    return m_impl.topk(k, cfn);
  }

  const value_type& default_value() const { return m_impl.default_value(); }
  #endif

 private:
  impl_type m_impl;
};
}  // namespace ygm::container
