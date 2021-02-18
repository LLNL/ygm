// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once
#include <cereal/archives/portable_binary.hpp>
#include <fstream>
#include <set>
#include <ygm/comm.hpp>
#include <ygm/container/detail/hash_partitioner.hpp>
#include <ygm/detail/ygm_ptr.hpp>
#include <ygm/utility.hpp>

namespace ygm::container::detail {
template <typename Key, typename SendBufferManager, typename Partitioner,
          typename Compare, class Alloc, int NumBanks, typename LockBankTag>
class set_impl {
public:
  using self_type = set_impl<Key, SendBufferManager, Partitioner, Compare,
                             Alloc, NumBanks, LockBankTag>;
  using key_type = Key;
  using lock_bank = ygm::lock_bank<NumBanks, LockBankTag>;

  Partitioner partitioner;

  set_impl(ygm::comm<SendBufferManager> &comm)
      : m_comm(comm), pthis(this), m_local_banked_set(NumBanks) {
    m_comm.barrier();
  }

  ~set_impl() { m_comm.barrier(); }

  void async_insert_multi(const key_type &key) {
    auto inserter = [](auto mailbox, int from, auto pset, const key_type &key,
                       const size_t bank) {
      auto l = lock_bank::mutex_lock(bank);
      pset->m_local_banked_set[bank].insert(key);
    };
    auto [dest, bank] = partitioner(key, m_comm.size(), NumBanks);
    m_comm.async(dest, inserter, pthis, key, bank);
  }

  void async_insert_unique(const key_type &key) {
    auto inserter = [](auto mailbox, int from, auto pset, const key_type &key,
                       const size_t bank) {
      auto l = lock_bank::mutex_lock(bank);
      if (pset->m_local_banked_set[bank].count(key) == 0) {
        pset->m_local_banked_set[bank].insert(key);
      }
    };
    auto [dest, bank] = partitioner(key, m_comm.size(), NumBanks);
    m_comm.async(dest, inserter, pthis, key, bank);
  }

  void async_erase(const key_type &key) {
    auto erase_wrapper = [](auto pcomm, int from, auto pset,
                            const key_type &key, const size_t bank) {
      auto l = lock_bank::mutex_lock(bank);
      pset->m_local_banked_set[bank].erase(key);
    };

    auto [dest, bank] = partitioner(key, m_comm.size(), NumBanks);
    m_comm.async(dest, erase_wrapper, pthis, key, bank);
  }

  template <typename Function> void for_all(Function fn) {
    m_comm.barrier();
    local_for_all(fn);
  }

  void clear() {
    m_comm.barrier();
#pragma omp parallel for
    for (int i = 0; i < NumBanks; ++i) {
      // Lock shouldn't be necessary here...
      auto l = lock_bank::mutex_lock(i);
      m_local_banked_set[i].clear();
    }
  }

  size_t size() {
    m_comm.barrier();
    size_t local_size{0};
#pragma omp parallel for reduction(+ : local_size)
    for (int i = 0; i < NumBanks; ++i) {
      local_size += m_local_banked_set[i].size();
    }
    return m_comm.all_reduce_sum(local_size);
  }

  size_t count(const key_type &key) {
    m_comm.barrier();
    auto [dest, bank] = partitioner(key, m_comm.size(), NumBanks);

    return m_comm.all_reduce_sum(m_local_banked_set[bank].count(key));
  }

  // Doesn't swap pthis.
  // should we check comm is equal? -- probably
  void swap(self_type &s) {
    m_comm.barrier();
    m_local_banked_set.swap(s.m_local_banked_set);
  }

  typename ygm::ygm_ptr<self_type> get_ygm_ptr() const { return pthis; }

  void serialize(const std::string &fname) {
    m_comm.barrier();
    std::string rank_fname = fname + std::to_string(m_comm.rank());
    std::ofstream os(rank_fname, std::ios::binary);
    cereal::PortableBinaryOutputArchive oarchive(os);
    oarchive(m_local_banked_set, m_comm.size());
  }

  void deserialize(const std::string &fname) {
    m_comm.barrier();

    std::string rank_fname = fname + std::to_string(m_comm.rank());
    std::ifstream is(rank_fname, std::ios::binary);

    cereal::PortableBinaryInputArchive iarchive(is);
    int comm_size;
    iarchive(m_local_banked_set, comm_size);

    if (comm_size != m_comm.size()) {
      m_comm.cerr0("Attempting to deserialize set_impl using communicator of "
                   "different size than serialized with");
    }
    if (m_local_banked_set.size() != NumBanks) {
      m_comm.cerr0("Attempting to deserialize set_impl with a different number "
                   "of banks than serialized with");
    }
  }

  // protected:
  template <typename Function> void local_for_all(Function fn) {
#pragma omp parallel for
    for (int i = 0; i < NumBanks; ++i) {
      auto l = lock_bank::mutex_lock(i);
      for (auto &item : m_local_banked_set[i]) {
        fn(item);
      }
    }
  }

  int owner(const key_type &key) const {
    auto [owner, bank] = partitioner(key, m_comm.size(), NumBanks);
    return owner;
  }
  set_impl() = delete;

  std::vector<std::multiset<key_type, Compare, Alloc>> m_local_banked_set;
  ygm::comm<SendBufferManager> m_comm;
  typename ygm::ygm_ptr<self_type> pthis;
};
} // namespace ygm::container::detail
