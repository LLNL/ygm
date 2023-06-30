// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once

#include <ygm/comm.hpp>
#include <ygm/container/map.hpp>
#include <ygm/detail/ygm_ptr.hpp>
#include <ygm/container/container_traits.hpp>

namespace ygm::container {

template <typename Key, typename Partitioner = detail::hash_partitioner<Key>,
          typename Compare = std::less<Key>,
          class Alloc      = std::allocator<std::pair<const Key, size_t>>>
class counting_set {
 public:
  using self_type           = counting_set<Key, Partitioner, Compare, Alloc>;
  using mapped_type         = size_t;
  using key_type            = Key;
  using size_type           = size_t;
  using ygm_for_all_types   = std::tuple< Key, size_t >;
  using ygm_container_type  = ygm::container::counting_set_tag;

  const size_type count_cache_size = 1024 * 1024;

  counting_set(ygm::comm &comm) : m_map(comm, mapped_type(0)), pthis(this) {
    m_count_cache.resize(count_cache_size, {key_type(), -1});
  }

  void async_insert(const key_type &key) { cache_insert(key); }

  // void async_erase(const key_type& key) { cache_erase(key); }

  template <typename Function>
  void for_all(Function fn) {
    m_map.for_all(fn);
  }

  void clear() { m_map.clear(); }

  size_type size() { return m_map.size(); }

  mapped_type count(const key_type &key) {
    m_map.comm().barrier();
    auto   vals = m_map.local_get(key);
    mapped_type local_count{0};
    for (auto v : vals) {
      local_count += v;
    }
    return m_map.comm().all_reduce_sum(local_count);
  }

  mapped_type count_all() {
    mapped_type local_count{0};
    for_all(
        [&local_count](const auto &key, auto &value) { local_count += value; });
    return m_map.comm().all_reduce_sum(local_count);
  }

  bool is_mine(const key_type &key) const { return m_map.is_mine(key); }

  template <typename CompareFunction>
  std::vector<std::pair<key_type, mapped_type>> topk(size_t          k,
                                                    CompareFunction cfn) {
    return m_map.topk(k, cfn);
  }

  template <typename STLKeyContainer>
  std::map<key_type, mapped_type> all_gather(const STLKeyContainer &keys) {
    return m_map.all_gather(keys);
  }

  std::map<key_type, mapped_type> all_gather(const std::vector<key_type> &keys) {
    return m_map.all_gather(keys);
  }

  typename ygm::ygm_ptr<self_type> get_ygm_ptr() const { return pthis; }

  void serialize(const std::string &fname) { m_map.serialize(fname); }
  void deserialize(const std::string &fname) { m_map.deserialize(fname); }

  ygm::comm &comm() { return m_map.comm(); }

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
    for (size_t i = 0; i < m_count_cache.size(); ++i) {
      if (m_count_cache[i].second > 0) {
        count_cache_flush(i);
      }
    }
    m_cache_empty = true;
  }
  counting_set() = delete;

  std::vector<std::pair<Key, int32_t>>                m_count_cache;
  bool                                                m_cache_empty = true;
  map<Key, mapped_type, Partitioner, Compare, Alloc>  m_map;
  typename ygm::ygm_ptr<self_type>                    pthis;
};

}  // namespace ygm::container
