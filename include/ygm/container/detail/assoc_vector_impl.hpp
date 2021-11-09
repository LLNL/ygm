#pragma once
#include <cereal/archives/json.hpp>
#include <cereal/types/utility.hpp>
#include <fstream>
#include <map>
#include <ygm/comm.hpp>
#include <ygm/container/detail/hash_partitioner.hpp>
#include <ygm/detail/ygm_ptr.hpp>


namespace ygm::container::detail {

template <typename Key, typename Value,
          typename Partitioner = detail::hash_partitioner<Key>,
          typename Compare     = std::less<Key>,
          class Alloc          = std::allocator<std::pair<const Key, Value>>>
class assoc_vector_impl {
 public:
  using key_type   = Key;
  using value_type = Value;
  using self_type  = assoc_vector_impl<Key, Value, Partitioner, Compare, Alloc>;

  Partitioner partitioner;

  assoc_vector_impl(ygm::comm &comm) : m_comm(comm), pthis(this), m_default_value{} {
    m_comm.barrier();
  }

  assoc_vector_impl(ygm::comm &comm, const value_type &dv)
      : m_comm(comm), pthis(this), m_default_value(dv) {
    m_comm.barrier();
  }

  assoc_vector_impl(const self_type &rhs)
      : m_comm(rhs.m_comm), pthis(this), m_default_value(rhs.m_default_value) {
    m_comm.barrier();
    m_local_map.insert(std::begin(rhs.m_local_map), std::end(rhs.m_local_map));
    m_comm.barrier();
  } 

  ~assoc_vector_impl() { m_comm.barrier(); }

  ygm::comm &comm() { return m_comm; }

  typename ygm::ygm_ptr<self_type> get_ygm_ptr() const { return pthis; }

  template <typename Function>
  void for_all(Function fn) {
    m_comm.barrier();
    local_for_all(fn);
  }

  template <typename Function>
  void local_for_all(Function fn) {
    std::for_each(m_local_map.begin(), m_local_map.end(), fn);
  }

  void clear() {
    m_comm.barrier();
    m_local_map.clear();
  }

  int owner(const key_type &key) const {
    auto [owner, rank] = partitioner(key, m_comm.size(), 1024);
    return owner;
  }

  void async_insert(const key_type& key, const value_type& value) {
    if (m_local_map.find(key) == m_local_map.end()) {
      //std::cout << "In insert." << std::endl;
      m_local_map.insert(std::make_pair(key, value));
    } else {
      m_local_map[key] = value;
    }
  }

  template <typename Visitor, typename... VisitorArgs>
  void async_visit_or_insert(const key_type& key, const value_type& value,
                              Visitor visitor, const VisitorArgs &...args) {
    auto visit_wrapper = [](auto pcomm, int from, auto pmap,
                       const key_type &key, const value_type &value, 
                       const VisitorArgs &...args) {
      Visitor *vis;
      pmap->local_visit_or_insert(key, value, *vis, from, args...);
    };
    int dest = owner(key);
    m_comm.async(dest, visit_wrapper, pthis, key, value,
                  std::forward<const VisitorArgs>(args)...);
  }

  template <typename Function, typename... VisitorArgs>
  void local_visit_or_insert(const key_type &key, const value_type &value,
                   Function &fn, const int from, const VisitorArgs &...args) {
    //std::cout << "Inside the assoc impl, lambda reached." << key << std::endl;
    if (m_local_map.find(key) == m_local_map.end()) {
      //std::cout << "In insert." << std::endl;
      m_local_map.insert(std::make_pair(key, value));
    } else {
      auto itr = m_local_map.find(key);
      ygm::meta::apply_optional(fn, std::make_tuple(pthis, from),
                                std::forward_as_tuple(itr, args...));
    }
  }

 protected:
  value_type                              m_default_value;
  std::map<key_type, value_type, Compare> m_local_map;
  ygm::comm                               m_comm;
  typename ygm::ygm_ptr<self_type>        pthis;
};
}
