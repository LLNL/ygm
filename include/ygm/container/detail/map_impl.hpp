// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once
#include <cereal/archives/portable_binary.hpp>
#include <fstream>
#include <map>
#include <ygm/comm.hpp>
#include <ygm/container/detail/hash_partitioner.hpp>
#include <ygm/detail/ygm_ptr.hpp>
#include <ygm/utility.hpp>

namespace ygm::container::detail {

template <typename Key, typename Value, typename SendBufferManager,
          typename Partitioner, typename Compare, class Alloc, int NumBanks,
          typename LockBankTag>
class map_impl {
public:
  using self_type = map_impl<Key, Value, SendBufferManager, Partitioner,
                             Compare, Alloc, NumBanks, LockBankTag>;
  using value_type = Value;
  using key_type = Key;
  using lock_bank = ygm::lock_bank<NumBanks, LockBankTag>;

  Partitioner partitioner;

  map_impl(ygm::comm<SendBufferManager> &comm)
      : m_comm(comm), pthis(this),
        m_local_banked_map(NumBanks), m_default_value{} {
    m_comm.barrier();
  }

  map_impl(ygm::comm<SendBufferManager> &comm, const value_type &dv)
      : m_comm(comm), pthis(this), m_local_banked_map(NumBanks),
        m_default_value(dv) {
    m_comm.barrier();
  }

  ~map_impl() { m_comm.barrier(); }

  void async_insert_unique(const key_type &key, const value_type &value) {
    auto inserter = [](auto mailbox, int from, auto map, const key_type &key,
                       const value_type &value, const size_t bank) {
      auto l = lock_bank::mutex_lock(bank);
      auto itr = map->m_local_banked_map[bank].find(key);
      if (itr != map->m_local_banked_map[bank].end()) {
        itr->second = value;
      } else {
        map->m_local_banked_map[bank].insert(std::make_pair(key, value));
      }
    };
    auto [dest, bank] = partitioner(key, m_comm.size(), NumBanks);
    m_comm.async(dest, inserter, pthis, key, value, bank);
  }

  void async_insert_multi(const key_type &key, const value_type &value) {
    auto inserter = [](auto mailbox, int from, auto map, const key_type &key,
                       const value_type &value, const size_t bank) {
      auto l = lock_bank::mutex_lock(bank);
      map->m_local_banked_map[bank].insert(std::make_pair(key, value));
    };
    auto [dest, bank] = partitioner(key, m_comm.size(), NumBanks);
    m_comm.async(dest, inserter, pthis, key, value, bank);
  }

  template <typename Visitor, typename... VisitorArgs>
  void async_visit(const key_type &key, Visitor visitor,
                   const VisitorArgs &... args) {
    auto visit_wrapper = [](auto pcomm, int from, auto pmap,
                            const key_type &key, const size_t bank,
                            const VisitorArgs &... args) {
      // TODO: Stuck holding lock if visitor sends a message and flushes a send
      // buffer...
      auto l = lock_bank::mutex_lock(bank);
      auto range = pmap->m_local_banked_map[bank].equal_range(key);
      if (range.first == range.second) { // check if not in range
        pmap->m_local_banked_map[bank].insert(
            std::make_pair(key, pmap->m_default_value));
        range = pmap->m_local_banked_map[bank].equal_range(key);
        ASSERT_DEBUG(range.first != range.second);
      }
      for (auto itr = range.first; itr != range.second; ++itr) {
        Visitor *v;
        (*v)(itr->first, itr->second, std::forward<const VisitorArgs>(args)...);
      }
    };

    auto [dest, bank] = partitioner(key, m_comm.size(), NumBanks);
    m_comm.async(dest, visit_wrapper, pthis, key, bank,
                 std::forward<const VisitorArgs>(args)...);
  }

  template <typename Visitor, typename... VisitorArgs>
  void async_visit_if_exists(const key_type &key, Visitor visitor,
                             const VisitorArgs &... args) {
    auto visit_wrapper = [](auto pcomm, int from, auto pmap,
                            const key_type &key, const size_t bank,
                            const VisitorArgs &... args) {
      Visitor *vis;
      pmap->local_visit(key, bank, *vis);
    };

    auto [dest, bank] = partitioner(key, m_comm.size(), NumBanks);
    m_comm.async(dest, visit_wrapper, pthis, key, bank,
                 std::forward<const VisitorArgs>(args)...);
  }

  void async_erase(const key_type &key) {
    auto erase_wrapper = [](auto pcomm, int from, auto pmap,
                            const key_type &key, const size_t bank) {
      pmap->local_erase(key, bank);
    };

    auto [dest, bank] = partitioner(key, m_comm.size(), NumBanks);
    m_comm.async(dest, erase_wrapper, pthis, key, bank);
  }

  size_t local_count(const key_type &key) {
    auto [dest, bank] = partitioner(key, m_comm.size(), NumBanks);
    return m_local_banked_map[bank].count(key);
  }

  template <typename Function> void for_all(Function fn) {
    m_comm.barrier();
    local_for_all(fn);
  }

  void clear() {
    m_comm.barrier();
#pragma omp parallel for
    for (int bank = 0; bank < NumBanks; ++bank) {
      // Lock shouldn't be necessary here...
      auto l = lock_bank::mutex_lock(bank);
      m_local_banked_map[bank].clear();
    }
  }

  size_t size() {
    m_comm.barrier();
    size_t local_size{0};
#pragma omp parallel for reduction(+ : local_size)
    for (int bank = 0; bank < NumBanks; ++bank) {
      local_size += m_local_banked_map[bank].size();
    }
    return m_comm.all_reduce_sum(local_size);
  }

  size_t count(const key_type &key) {
    m_comm.barrier();
    auto [dest, bank] = partitioner(key, m_comm.size(), NumBanks);
    return m_comm.all_reduce_sum(m_local_banked_map[bank].count(key));
  }

  // Doesn't swap pthis.
  // should we check comm is equal? -- probably
  void swap(self_type &s) {
    m_comm.barrier();
    std::swap(m_default_value, s.m_default_value);
    m_local_banked_map.swap(s.m_local_banked_map);
  }

  template <typename STLKeyContainer, typename MapKeyValue>
  void all_gather(const STLKeyContainer &keys, MapKeyValue &output) {
    ygm::ygm_ptr<MapKeyValue> preturn(&output);

    // Locking on every insert into output
    std::mutex mtx;
    ygm::ygm_ptr<std::mutex> pmutex(&mtx);

    auto fetcher = [](auto pcomm, int from, const key_type &key,
                      const size_t bank, auto pmap, auto pcont, auto pmutex) {
      auto returner = [](auto pcomm, int from, const key_type &key,
                         const std::vector<value_type> &values, auto pcont,
                         auto pmutex) {
        std::scoped_lock lock(*pmutex);
        for (const auto &v : values) {
          pcont->insert(std::make_pair(key, v));
        }
      };
      auto values = pmap->local_get(key, bank);
      pcomm->async(from, returner, key, values, pcont, pmutex);
    };

    m_comm.barrier();
    for (const auto &key : keys) {
      auto [dest, bank] = partitioner(key, m_comm.size(), NumBanks);
      m_comm.async(dest, fetcher, key, bank, pthis, preturn, pmutex);
    }
    m_comm.barrier();
  }

  typename ygm::ygm_ptr<self_type> get_ygm_ptr() const { return pthis; }

  void serialize(const std::string &fname) {
    m_comm.barrier();
    std::string rank_fname = fname + std::to_string(m_comm.rank());
    std::ofstream os(rank_fname, std::ios::binary);
    cereal::PortableBinaryOutputArchive oarchive(os);
    oarchive(m_local_banked_map, m_default_value, m_comm.size());
  }

  void deserialize(const std::string &fname) {
    m_comm.barrier();

    std::string rank_fname = fname + std::to_string(m_comm.rank());
    std::ifstream is(rank_fname, std::ios::binary);

    cereal::PortableBinaryInputArchive iarchive(is);
    int comm_size;
    iarchive(m_local_banked_map, m_default_value, comm_size);

    if (comm_size != m_comm.size()) {
      m_comm.cerr0("Attempting to deserialize map_impl using communicator of "
                   "different size than serialized with");
    }
    if (m_local_banked_map.size() != NumBanks) {
      m_comm.cerr0("Attempting to deserialize map_impl with a different number "
                   "of banks than serialized with");
    }
  }

  int owner(const key_type &key) const {
    auto [owner, rank] = partitioner(key, m_comm.size(), 1024);
    return owner;
  }

  bool is_mine(const key_type &key) const {
    return owner(key) == m_comm.rank();
  }

  std::vector<value_type> local_get(const key_type &key, const size_t bank) {
    std::vector<value_type> to_return;

    auto l = lock_bank::mutex_lock(bank);
    auto range = m_local_banked_map[bank].equal_range(key);
    for (auto itr = range.first; itr != range.second; ++itr) {
      to_return.push_back(itr->second);
    }

    return to_return;
  }

  std::vector<value_type> local_get(const key_type &key) {
    auto [dest, bank] = partitioner(key, m_comm.size(), NumBanks);
    return local_get(key, bank);
  }

  template <typename Function>
  void local_visit(const key_type &key, const size_t bank, Function &fn) {
    auto l = lock_bank::mutex_lock(bank);
    auto range = m_local_banked_map[bank].equal_range(key);
    std::for_each(range.first, range.second, fn);
  }

  template <typename Function>
  void local_visit(const key_type &key, Function &fn) {
    auto [dest, bank] = partitioner(key, m_comm.size(), NumBanks);
  }

  void local_erase(const key_type &key, const size_t bank) {
    auto l = lock_bank::mutex_lock(bank);
    m_local_banked_map[bank].erase(key);
  }

  void local_erase(const key_type &key) {
    auto [dest, bank] = partitioner(key, m_comm.size(), NumBanks);
    local_erase(key, bank);
  }

  void local_clear() {
#pragma omp parallel for
    for (int bank = 0; bank < NumBanks; ++bank) {
      auto l = lock_bank::mutex_lock(bank);
      m_local_banked_map[bank].clear();
    }
  }

  size_t local_size() const {
    size_t cumulative_size{0};
    for (int bank = 0; bank < NumBanks; ++bank) {
      auto l = lock_bank::mutex_lock(bank);
      cumulative_size += m_local_banked_map[bank].size();
    }
    return cumulative_size;
  }

  size_t local_count(const key_type &k) const {
    auto [dest, bank] = partitioner(k, m_comm.size(), NumBanks);
    return m_local_banked_map[bank].count(k);
  }

  ygm::comm<SendBufferManager> &comm() { return m_comm; }

  template <typename Function> void local_for_all(Function fn) {
#pragma omp parallel for
    for (int bank = 0; bank < NumBanks; ++bank) {
      auto l = lock_bank::mutex_lock(bank);
      for (const auto &item : m_local_banked_map[bank]) {
        fn(item);
      }
    }
  }

  // Not parallel
  template <typename CompareFunction>
  std::vector<std::pair<key_type, value_type>> topk(size_t k,
                                                    CompareFunction cfn) {
    using vec_type = std::vector<std::pair<key_type, value_type>>;
    vec_type local_topk;
    for (int bank = 0; bank < NumBanks; ++bank) {
      auto l = lock_bank::mutex_lock(bank);
      for (const auto &kv : m_local_banked_map[bank]) {
        local_topk.push_back(kv);
        std::sort(local_topk.begin(), local_topk.end(), cfn);
        if (local_topk.size() > k) {
          local_topk.pop_back();
        }
      }
    }

    auto to_return = m_comm.all_reduce(
        local_topk, [cfn, k](const vec_type &va, const vec_type &vb) {
          vec_type out(va.begin(), va.end());
          out.push_back(vb.begin(), vb.end());
          std::sort(out.begin(), out.end(), cfn);
          while (out.size() > k) {
            out.pop_back();
          }
        });
    return to_return;
  }

protected:
  map_impl() = delete;

  value_type m_default_value;
  std::vector<std::multimap<key_type, value_type, Compare, Alloc>>
      m_local_banked_map;
  ygm::comm<SendBufferManager> m_comm;
  typename ygm::ygm_ptr<self_type> pthis;
};
} // namespace ygm::container::detail
