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

  void async_visit_or_insert(const key_type& row, const key_type& col, 
                             const value_type& value) {
    auto row_inserter = [](auto mailbox, int from, auto pmaptrix,
                       const key_type &row, const key_type &col,
                       const value_type &value) {
      /* Default construct..  */
      if (pmaptrix->m_row_map.find(row) == pmaptrix->m_row_map.end()) {
        // row not found.
        pmaptrix->m_row_map[row].insert(std::make_pair(col, value)); 
      } else { 
        inner_map_type &col_map = pmaptrix->m_row_map[row];
        auto col_itr = col_map.find(col);
        if (col_map.find(col) == col_map.end()) {
          // column not found, insert.
          col_map.insert(std::make_pair(col, value));
        } else {
          // found and update.
          col_map[col] = value;
        } 
      }
    };

    int dest = owner(row, col);
    m_comm.async(dest, row_inserter, pthis, row, col, value);
  }

  #ifdef old_impl
  template <typename Visitor, typename... VisitorArgs>
  void async_visit_col_const(const key_type &col, Visitor visitor,
                             const VisitorArgs &...args) {

    /* !!!!!!!!!!!!!!  This is weird  !!!!!!!!!!!!!!!!!!!!! */
    int  dest = owner(col, col);

    auto col_visit_wrapper = [](auto pcomm, int from, auto pmaptrix,
                            const key_type &col, const VisitorArgs &...args) {
      Visitor *vis;
      /* Assume the data of the column to be in
        * the same local node. */
      pmaptrix->col_local_for_all(col, *vis, from, args...);
    };

    m_comm.async(dest, col_visit_wrapper, pthis, col,
                 std::forward<const VisitorArgs>(args)...);
  }

  /* Expect the row data and column data of an identifier to be 
      ** stored in the same node. */
  template <typename Function, typename... VisitorArgs>
  void col_local_for_all(const key_type &col, Function fn, const int from,
                   const VisitorArgs &...args) {
    //inner_map_type &row_map = m_col_map.find(col)->second;
    //std::for_each(row_map.begin(), row_map.end(), fn);
    
    auto fn_wrapper = [fn, ...args = std::forward<const VisitorArgs>(args)](auto &e) {
      std::cout << "ColVisit: Using key: " << e.first << std::endl;
      key_type outer_key        = e.first;
      inner_map_type &inner_map = e.second;
      for (auto itr = inner_map.begin(); itr != inner_map.end(); ++itr) {
        key_type inner_key      = itr->first;
        value_type value        = itr->second;
        fn(outer_key, inner_key, value, args...);
      }
    }; 

    std::for_each(m_col_map.begin(), m_col_map.end(), fn_wrapper);
  }
  #endif 

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

  void local_clear() { m_local_map.clear(); }

  size_t local_size() const { return m_local_map.size(); }

  /* When may this be used? */
  //size_t local_count(const key_type &key) const { 
    //return (m_local_map.find(key)->first.count(k)+m_local_map.find(key)->second.count(k)); 
  //}
  #endif

  #ifdef row_col_ownership
  int owner(const key_type &row, const key_type &col) const {
    auto [owner, rank] = partitioner(row, col, m_comm.size(), 1024);
    return owner;
  }

  bool is_mine(const key_type &row, const key_type &col) const {
    return owner(row, col) == m_comm.rank();
  }
  #endif


  #ifdef serialize
  typename ygm::ygm_ptr<self_type> get_ygm_ptr() const { return pthis; }

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
