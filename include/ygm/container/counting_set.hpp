// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once

#include <ygm/comm.hpp>
#include <ygm/container/map.hpp>
#include <ygm/detail/ygm_ptr.hpp>

namespace ygm::container {

template <typename Key,
          typename SendBufferManager = ygm::locking_send_buffer_manager,
          typename Partitioner = detail::hash_partitioner<Key>,
          typename Compare = std::less<Key>,
          class Alloc = std::allocator<std::pair<const Key, size_t>>,
          int NumBanks = 1024, typename LockBankTag = ygm::DefaultLockBankTag>
class counting_set {
public:
  using self_type = counting_set<Key, SendBufferManager, Partitioner, Compare,
                                 Alloc, NumBanks, LockBankTag>;
  using key_type = Key;
  using value_type = size_t;
  using lock_bank = ygm::lock_bank<NumBanks, LockBankTag>;

  const size_t count_cache_size = 1024;

  counting_set(ygm::comm<SendBufferManager> &comm)
      : m_map(comm, value_type(0)), m_banked_count_cache(NumBanks),
        pthis(this) {
#pragma omp parallel for
    for (int i = 0; i < NumBanks; ++i) {
      m_banked_count_cache[i].resize(count_cache_size, {key_type(), -1});
    }
  }

  void async_insert(const key_type &key) { cache_insert(key); }

  // void async_erase(const key_type& key) { cache_erase(key); }

  template <typename Function> void for_all(Function fn) {
    count_cache_flush_all();
    m_map.for_all(fn);
  }

  void clear() {
    count_cache_flush_all();
    m_map.clear();
  }

  size_t size() {
    count_cache_flush_all();
    return m_map.size();
  }

  size_t count(const key_type &key) {
    count_cache_flush_all();
    m_map.comm().barrier();
    auto vals = m_map.local_get(key);
    size_t local_count{0};
    for (auto v : vals) {
      local_count += v;
    }
    return m_map.comm().all_reduce_sum(local_count);
  }

  size_t count_all() {
    count_cache_flush_all();
    std::atomic<size_t> local_count{0};
    for_all([&local_count](const auto &kv) { local_count += kv.second; });
    size_t nonatomic = local_count;
    return m_map.comm().all_reduce_sum(nonatomic);
  }

  bool is_mine(const key_type &key) const { return m_map.is_mine(key); }

  template <typename STLKeyContainer>
  std::map<key_type, value_type> all_gather(const STLKeyContainer &keys) {
    count_cache_flush_all();
    return m_map.all_gather(keys);
  }

  std::map<key_type, value_type> all_gather(const std::vector<key_type> &keys) {
    count_cache_flush_all();
    return m_map.all_gather(keys);
  }

  void serialize(const std::string &fname) {
    count_cache_flush_all();
    m_map.serialize(fname);
  }
  void deserialize(const std::string &fname) {
    count_cache_flush_all();
    m_map.deserialize(fname);
  }

private:
  void cache_erase(const key_type &key) {
    auto [bank, slot] = location(key);
    auto l = lock_bank::mutex_lock(bank);

    if (m_banked_count_cache[bank][slot].second != -1 &&
        m_banked_count_cache[bank][slot].first == key) {
      // Key was cached, clear cache
      m_banked_count_cache[bank][slot].second = -1;
      m_banked_count_cache[bank][slot].first = key_type();
    }
    m_map.async_erase(key);
  }
  void cache_insert(const key_type &key) {
    auto [bank, slot] = location(key);
    auto l = lock_bank::mutex_lock(bank);

    if (m_banked_count_cache[bank][slot].second == -1) {
      m_banked_count_cache[bank][slot].first = key;
      m_banked_count_cache[bank][slot].second = 1;
    } else {
      // flush slot, fill with key
      ASSERT_DEBUG(m_banked_count_cache[bank][slot].second > 0);
      if (m_banked_count_cache[bank][slot].first == key) {
        m_banked_count_cache[bank][slot].second++;
      } else {
        count_cache_flush(bank, slot);
        ASSERT_DEBUG(m_banked_count_cache[bank][slot].second == -1);
        m_banked_count_cache[bank][slot].first = key;
        m_banked_count_cache[bank][slot].second = 1;
      }
    }
    if (m_banked_count_cache[bank][slot].second ==
        std::numeric_limits<int32_t>::max()) {
      count_cache_flush(bank, slot);
    }
  }

  void count_cache_flush(size_t bank, size_t slot) {
    // Locking assumed to happen outside count_cache_flush
    auto key = m_banked_count_cache[bank][slot].first;
    auto cached_count = m_banked_count_cache[bank][slot].second;
    ASSERT_DEBUG(cached_count > 0);
    m_map.async_visit(key,
                      [](const key_type &k, size_t &count, int32_t to_add) {
                        count += to_add;
                      },
                      cached_count);
    m_banked_count_cache[bank][slot].first = key_type();
    m_banked_count_cache[bank][slot].second = -1;
  }

  void count_cache_flush_all() {
#pragma omp parallel for
    for (int bank = 0; bank < NumBanks; ++bank) {
      auto l = lock_bank::mutex_lock(bank);
      for (size_t slot = 0; slot < m_banked_count_cache[bank].size(); ++slot) {
        if (m_banked_count_cache[bank][slot].second > 0) {
          count_cache_flush(bank, slot);
        }
      }
    }
  }

  std::pair<size_t, size_t> location(const key_type &key) {
    return partitioner(key, NumBanks, count_cache_size);
  }
  counting_set() = delete;

  std::vector<std::vector<std::pair<Key, int32_t>>> m_banked_count_cache;
  map<Key, value_type, SendBufferManager, Partitioner, Compare, Alloc, NumBanks,
      LockBankTag>
      m_map;
  typename ygm::ygm_ptr<self_type> pthis;

  Partitioner partitioner;
};

} // namespace ygm::container
