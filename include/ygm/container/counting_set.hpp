// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once

#include <ygm/comm.hpp>
#include <ygm/container/container_traits.hpp>
#include <ygm/container/map.hpp>
#include <ygm/container/detail/base_iteration.hpp>
#include <ygm/container/detail/base_count.hpp>
#include <ygm/container/detail/base_misc.hpp>
#include <ygm/detail/ygm_ptr.hpp>


namespace ygm::container {

template<typename Key>
class counting_set : public detail::base_count<counting_set<Key>, std::tuple<Key,size_t>>,
                     public detail::base_misc<counting_set<Key>, std::tuple<Key,size_t>>,
                     public detail::base_iteration<counting_set<Key>, std::tuple<Key,size_t>>
        {
  friend class detail::base_misc<counting_set<Key>, std::tuple<Key,size_t>>;

 public:
  using self_type         = counting_set<Key>;
  using mapped_type       = size_t;
  using key_type          = Key;
  using size_type         = size_t;
  using for_all_args = std::tuple<Key, size_t>;
  using container_type    = ygm::container::counting_set_tag;

  const size_type count_cache_size = 1024 * 1024;

  counting_set(ygm::comm &comm)
      : m_map(comm /*, mapped_type(0)*/), m_comm(comm), partitioner(m_map.partitioner), pthis(this) {
    m_count_cache.resize(count_cache_size, {key_type(), -1});
  }

  counting_set() = delete;

  void async_insert(const key_type &key) { cache_insert(key); }

  // void async_erase(const key_type& key) { cache_erase(key); }

  template <typename Function> 
  void local_for_all(Function fn) {
    m_map.local_for_all(fn);
  }

  void local_clear() { // What to do here
    m_map.local_clear();
    clear_cache();
  }
 
  using detail::base_misc<counting_set<Key>, for_all_args>::clear;

  void clear() {
    local_clear();
    m_comm.barrier();
  }

  size_t local_size() const { return m_map.local_size(); }

  mapped_type local_count(const key_type &key) const {
    auto        vals = m_map.local_get(key);
    mapped_type local_count{0};
    for (auto v : vals) {
      local_count += v;
    }
    return local_count;
  }

  mapped_type count_all() {
    mapped_type local_count{0};
    local_for_all(
        [&local_count](const auto &key, auto &value) { local_count += value; });
    return ygm::sum(local_count, m_map.comm());
  }

  // bool is_mine(const key_type &key) const { return m_map.is_mine(key); }

  template <typename CompareFunction>
  std::vector<std::pair<key_type, mapped_type>> topk(size_t          k,
                                                     CompareFunction cfn) {
    return m_map.topk(k, cfn);
  }

  // template <typename STLKeyContainer>
  // std::map<key_type, mapped_type> all_gather(const STLKeyContainer &keys) {
  //   return m_map.all_gather(keys);
  // }

  std::map<key_type, mapped_type> key_gather(
      const std::vector<key_type> &keys) {
    return m_map.key_gather(keys);
  }

  typename ygm::ygm_ptr<self_type> get_ygm_ptr() const { return pthis; }

  void serialize(const std::string &fname) { m_map.serialize(fname); }
  void deserialize(const std::string &fname) { m_map.deserialize(fname); }

  detail::hash_partitioner<std::hash<key_type>> partitioner;


 private:
  void cache_erase(const key_type &key) {
    size_t slot = std::hash<key_type>{}(key) % count_cache_size;
    if (m_count_cache[slot].second != -1 && m_count_cache[slot].first == key) {
      // Key was cached, clear cache
      m_count_cache[slot].second = -1;
      m_count_cache[slot].first  = key_type();
    }
    m_map.async_erase(key);
  }

  void cache_insert(const key_type &key) {
    if (m_cache_empty) {
      m_cache_empty = false;
      m_map.comm().register_pre_barrier_callback(
          [this]() { this->count_cache_flush_all(); });
    }
    size_t slot = std::hash<key_type>{}(key) % count_cache_size;
    if (m_count_cache[slot].second == -1) {
      m_count_cache[slot].first  = key;
      m_count_cache[slot].second = 1;
    } else {
      // flush slot, fill with key
      ASSERT_DEBUG(m_count_cache[slot].second > 0);
      if (m_count_cache[slot].first == key) {
        m_count_cache[slot].second++;
      } else {
        count_cache_flush(slot);
        ASSERT_DEBUG(m_count_cache[slot].second == -1);
        m_count_cache[slot].first  = key;
        m_count_cache[slot].second = 1;
      }
    }
    if (m_count_cache[slot].second == std::numeric_limits<int32_t>::max()) {
      count_cache_flush(slot);
    }
  }

  void count_cache_flush(size_t slot) {
    auto key          = m_count_cache[slot].first;
    auto cached_count = m_count_cache[slot].second;
    ASSERT_DEBUG(cached_count > 0);
    m_map.async_visit(
        key,
        [](const key_type &key, size_t &count, int32_t to_add) {
          count += to_add;
        },
        cached_count);
    m_count_cache[slot].first  = key_type();
    m_count_cache[slot].second = -1;
  }

  void count_cache_flush_all() {
    if (!m_cache_empty) {
      for (size_t i = 0; i < m_count_cache.size(); ++i) {
        if (m_count_cache[i].second > 0) {
          count_cache_flush(i);
        }
      }
      m_cache_empty = true;
    }
  }

  void clear_cache() {
    for (size_t i = 0; i < m_count_cache.size(); ++i) {
      m_count_cache[i].first  = key_type();
      m_count_cache[i].second = -1;
    }
    m_cache_empty = true;
  }


  ygm::comm                            &m_comm;
  std::vector<std::pair<Key, int32_t>>  m_count_cache;
  bool                                  m_cache_empty = true;
  map<Key, mapped_type>                 m_map;
  typename ygm::ygm_ptr<self_type>      pthis;
};

}  // namespace ygm::container
