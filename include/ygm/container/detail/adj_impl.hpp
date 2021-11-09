#pragma once
#include <cereal/archives/json.hpp>
#include <cereal/types/utility.hpp>
#include <fstream>
#include <map>
#include <ygm/comm.hpp>
#include <ygm/container/detail/hash_partitioner.hpp>
#include <ygm/detail/ygm_ptr.hpp>

namespace ygm::container::detail {

/* Should adj impl also have an abstraction? */
template <typename Key, typename Value,
          typename Partitioner = detail::hash_partitioner<Key>,
          typename Compare     = std::less<Key>,
          class Alloc          = std::allocator<std::pair<const Key, Value>>>
class adj_impl {
 public:
  using key_type   = Key;
  using value_type = Value;
  using inner_map_type = std::map<Key, Value>;
  using self_type  = adj_impl<Key, Value, Partitioner, Compare, Alloc>;

  Partitioner partitioner;

  adj_impl(ygm::comm &comm) : m_comm(comm), pthis(this), m_default_value{} {
    m_comm.barrier();
  }

  int owner(const key_type &key) const {
    auto [owner, rank] = partitioner(key, m_comm.size(), 1024);
    return owner;
  }

  int owner(const key_type &row, const key_type &col) const {
    auto [owner, rank] = partitioner(row, col, m_comm.size(), 1024);
    return owner;
  }

  bool is_mine(const key_type &row, const key_type &col) const {
    return owner(row) == m_comm.rank();
  }

  ygm::comm &comm() { return m_comm; }

  std::map<key_type, inner_map_type, Compare> &adj() { return m_map; }

  ~adj_impl() { m_comm.barrier(); }

  void async_insert(const key_type& row, const key_type& col, const value_type& value) {
    auto inserter = [](auto mailbox, int from, auto padj,
                       const key_type &row, const key_type &col,
                       const value_type &value) {
      padj->m_map[row].insert(std::make_pair(col, value));
    };
    int dest = owner(row);
    m_comm.async(dest, inserter, pthis, row, col, value);
  }

  void clear() {
    m_comm.barrier();
    m_map.clear();
  }

  template <typename Function>
  void for_all(Function fn) {
    m_comm.barrier();
    local_for_all(fn);
  }

  template <typename Function>
  void local_for_all(Function fn) {
    for (auto itr = m_map.begin(); itr != m_map.end(); ++itr) {
      //std::cout << "Using key: " << itr->first << std::endl;
      key_type outer_key        = itr->first;
      inner_map_type &inner_map = itr->second;
      for (auto inner_itr = inner_map.begin(); inner_itr != inner_map.end(); ++inner_itr) {
        key_type inner_key      = inner_itr->first;
        value_type value        = inner_itr->second;
        fn(outer_key, inner_key, value);
      }
    }
  }

  template <typename Visitor, typename... VisitorArgs>
  void async_visit_if_exists(const key_type &row, const key_type &col,
          Visitor visitor, const VisitorArgs &...args) {

    auto visit_wrapper = [](auto pcomm, int from, auto padj,
                            const key_type &row, const key_type &col, const VisitorArgs &...args) {
      Visitor *vis;
      padj->local_visit(row, col, *vis, from, args...);
    };

    int dest = owner(row);
    m_comm.async(dest, visit_wrapper, pthis, row, col,
                 std::forward<const VisitorArgs>(args)...);
  }

  template <typename Function, typename... VisitorArgs>
  void local_visit(const key_type &row, const key_type &col,
                   Function &fn, const int from, const VisitorArgs &...args) {
    /* Fetch the row map, key: col id, value: val. */
    inner_map_type &inner_map = m_map[row];
    value_type value          = inner_map[col];

    /* Assuming this changes the value at row, col. */
    ygm::meta::apply_optional(fn, std::make_tuple(pthis, from),
                                std::forward_as_tuple(row, col, value, args...));
  }

  template <typename Visitor, typename... VisitorArgs>
  void async_visit_const(const key_type &key, Visitor visitor,
                             const VisitorArgs &...args) {

    int  dest = owner(key);
    auto visit_wrapper = [](auto pcomm, int from, auto padj,
                            const key_type &key, const VisitorArgs &...args) {
      Visitor *vis;
      padj->adj_local_for_all(key, *vis, from, args...);
    };

    m_comm.async(dest, visit_wrapper, pthis, key,
                 std::forward<const VisitorArgs>(args)...);
  }

  template <typename Function, typename... VisitorArgs>
  void adj_local_for_all(const key_type &key, Function fn, const int from,
                   const VisitorArgs &...args) {
    auto &inner_map = m_map[key];
    for (auto itr = inner_map.begin(); itr != inner_map.end(); ++itr) {
      //std::cout << "ColVisit: Using key: " << itr->first << std::endl;
      key_type outer_key  = key;       
      key_type inner_key  = itr->first;
      value_type value    = itr->second;
      fn(outer_key, inner_key, value, args...);
    }
  }

  template <typename Visitor, typename... VisitorArgs>
  void async_visit_or_insert(const key_type& row, const key_type& col, const value_type& value, 
                              Visitor visitor, const VisitorArgs &...args) {

    std::cout << "Inside the adj impl." << std::endl;
    auto visit_wrapper = [](auto pcomm, int from, auto padj,
                       const key_type &row, const key_type &col,
                       const value_type &value, const VisitorArgs &...args) {
      //Apply Visitor.. 
      Visitor *vis;
      padj->local_visit_or_insert(row, col, value, *vis, from, args...); 
    };

    int dest = owner(row);
    m_comm.async(dest, visit_wrapper, pthis, row, col, value, 
                  std::forward<const VisitorArgs>(args)...);
  }

  /* Do we really need a value here? */
  template <typename Function, typename... VisitorArgs>
  void local_visit_or_insert(const key_type &row, const key_type &col, const value_type &value,
                   Function &fn, const int from, const VisitorArgs &...args) {
    std::cout << "Inside the local adj impl, lambda reached." << row << col << std::endl;
    /* Fetch the row map, key: col id, value: val. */
    inner_map_type &inner_map = m_map[row];
    if (inner_map.find(col) == inner_map.end()) {
      std::cout << "In insert." << std::endl;
      inner_map.insert(std::make_pair(col, value));
    } else {
      value_type value = inner_map[col];
      ygm::meta::apply_optional(fn, std::make_tuple(pthis, from),
                                std::forward_as_tuple(row, col, value, args...));
    }
  }

 protected: 
  value_type                                  m_default_value;
  std::map<key_type, inner_map_type, Compare> m_map;
  ygm::comm                                   m_comm;
  typename ygm::ygm_ptr<self_type>            pthis;
};
} // namespace ygm::container::detail
