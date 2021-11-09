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
#include <ygm/container/detail/adj_impl.hpp>

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
  using inner_map_type = std::map<Key, Value>;
  using adj_impl   = detail::adj_impl<key_type, value_type, Partitioner, Compare, Alloc>;

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
    /* changing order of row col to allow 
      * for csc insert. */
    m_csc.async_insert(col, row, value);
  }

  //*** TBD: Should you support this function? ***//
  ygm::comm &comm() { return m_comm; }

  /* For all is expected to be const on csc. */
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
    /* Can inserts or updates be allowed only if as a udf, you have access to the 
      * m_csr/m_csc object? */
    m_csr.async_visit_if_exists(row, col, visitor, std::forward<const VisitorArgs>(args)...);
    m_csc.async_visit_if_exists(col, row, visitor, std::forward<const VisitorArgs>(args)...);
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
    m_csc.async_visit_const(col, visitor, std::forward<const VisitorArgs>(args)...);
  }

  template <typename Visitor, typename... VisitorArgs>
  void async_visit_or_insert(const key_type& row, const key_type& col, const value_type &value, 
                                Visitor visitor, const VisitorArgs&... args) {
    //std::cout << "Inside the impl." << std::endl;
    m_csr.async_visit_or_insert(row, col, value, visitor, std::forward<const VisitorArgs>(args)...);
    //m_csc.async_visit_or_insert(col, row, value, visitor, std::forward<const VisitorArgs>(args)...);
  }

  typename ygm::ygm_ptr<self_type> get_ygm_ptr() const { return pthis; }

  void local_clear() { 
    m_csr.clear(); 
    m_csc.clear(); 
  }

  /*****************************************************************************************/
  /*                                     TO BE IMPLEMENTED                                 */
  /*****************************************************************************************/

  #ifdef other_local_fns
  void async_erase(const key_type &row, const key_type &col) {
    auto erase_wrapper = [](auto pcomm, int from, auto pmaptrix,
                            const key_type &row, const key_type &col) { pmaptrix->local_erase(row, col); };
    int  dest          = owner(row, col);
    m_comm.async(dest, erase_wrapper, pthis, row, col);

    /* Erasing transpose entry. */
    dest               = owner(col, row);
    m_comm.async(dest, erase_wrapper, pthis, col, row);
  }

  /* designing erase to only wipe out the exact entry. */
  void local_erase(const key_type &row, const key_type &col) { 
    inner_map_type row_map = m_local_map.find(row);
    row_map.erase(col); 
  }

  /* When may this be used? */
  //size_t local_count(const key_type &key) const { 
    //return (m_local_map.find(key)->first.count(k)+m_local_map.find(key)->second.count(k)); 
  //}
  #endif

  #ifdef serialize
  void serialize(const std::string &fname) {
    m_comm.barrier();
    std::string   rank_fname = fname + std::to_string(m_comm.rank());
    std::ofstream os(rank_fname, std::ios::binary);
    cereal::JSONOutputArchive oarchive(os);
    oarchive(m_local_map, m_default_value, m_comm.size());
  }

  void deserialize(const std::string &fname) {
    m_comm.barrier();

    std::string   rank_fname = fname + std::to_string(m_comm.rank());
    std::ifstream is(rank_fname, std::ios::binary);

    cereal::JSONInputArchive iarchive(is);
    int                      comm_size;
    iarchive(m_local_map, m_default_value, comm_size);

    if (comm_size != m_comm.size()) {
      m_comm.cerr0(
          "Attempting to deserialize map_impl using communicator of "
          "different size than serialized with");
    }
  }
  #endif
  
  //==

 protected:
  maptrix_impl() = delete;

  value_type                                          m_default_value;
  /* In maptrix, we assume that a key is associated with 
    * two maps - the row map which contains the entries along the row, 
    * the column map which contains entries along the column. */
  /* inner_map_type is expected to be swapped out with other classes. */
  /* Within the tuple associated with a key, 
    * the first map represents the row: <col-identifier, value>, 
    * the second map represents the col: <row-identifier, value>. */

  //std::map<key_type, inner_map_type, Compare>        m_row_map;      
  adj_impl                                            m_csr;      
  adj_impl                                            m_csc; 
  ygm::comm                                           m_comm;
  typename ygm::ygm_ptr<self_type>                    pthis;
};
}  // namespace ygm::container::detail
