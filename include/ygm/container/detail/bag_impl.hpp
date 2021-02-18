// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once
#include <cereal/archives/portable_binary.hpp>
#include <chrono>
#include <fstream>
#include <vector>
#include <x86intrin.h>
#include <ygm/comm.hpp>
#include <ygm/detail/ygm_ptr.hpp>
#include <ygm/utility.hpp>

namespace ygm::container::detail {
template <typename Item, typename SendBufferManager, typename Alloc,
          int NumBanks, typename LockBankTag>
class bag_impl {
public:
  using value_type = Item;
  using self_type =
      bag_impl<Item, SendBufferManager, Alloc, NumBanks, LockBankTag>;
  using lock_bank = ygm::lock_bank<NumBanks, LockBankTag>;

  bag_impl(ygm::comm<SendBufferManager> &comm)
      : m_comm(comm), m_local_banked_bag(NumBanks), pthis(this) {
    m_comm.barrier();
    m_round_robin = m_comm.rank();
  }

  ~bag_impl() { m_comm.barrier(); }

  void async_insert(const value_type &item) {
    auto inserter = [](auto mailbox, int from, auto bag,
                       const value_type &item) {
      int bank = __rdtsc() % NumBanks;
      auto l = lock_bank::mutex_lock(bank);
      bag->m_local_banked_bag[bank].push_back(item);
    };
    int dest = (m_round_robin++) % m_comm.size();
    m_comm.async(dest, inserter, pthis, item);
  }

  template <typename Function> void for_all(Function fn) {
    m_comm.barrier();
    local_for_all(fn);
  }

  void clear() {
    m_comm.barrier();
#pragma omp parallel for
    for (int i = 0; i < NumBanks; ++i) {
      // Lock should be unnecessary here, but added just in case I missed
      // something...
      lock_bank::mutex_lock(i);
      m_local_banked_bag[i].clear();
    }
  }

  size_t size() {
    m_comm.barrier();
    size_t local_size{0};
#pragma omp parallel for reduction(+ : local_size)
    for (int i = 0; i < NumBanks; ++i) {
      local_size += m_local_banked_bag[i].size();
    }
    return m_comm.all_reduce_sum(local_size);
  }

  void swap(self_type &s) {
    m_comm.barrier();
    m_local_banked_bag.swap(s.m_local_banked_bag);
  }

  void serialize(const std::string &fname) {
    m_comm.barrier();
    std::string rank_fname = fname + std::to_string(m_comm.rank());
    std::ofstream os(rank_fname, std::ios::binary);
    cereal::PortableBinaryOutputArchive oarchive(os);
    oarchive(m_local_banked_bag, m_round_robin, m_comm.size());
  }

  void deserialize(const std::string &fname) {
    m_comm.barrier();

    std::string rank_fname = fname + std::to_string(m_comm.rank());
    std::ifstream is(rank_fname, std::ios::binary);

    cereal::PortableBinaryInputArchive iarchive(is);
    int comm_size;
    iarchive(m_local_banked_bag, m_round_robin, comm_size);

    if (comm_size != m_comm.size()) {
      m_comm.cerr0("Attempting to deserialize bag_impl using communicator of "
                   "different size than serialized with");
    }
    if (m_local_banked_bag.size() != NumBanks) {
      m_comm.cerr0("Attempting to deserialize bag_impl with a different number "
                   "of banks than serialized with");
    }
  }

  template <typename Function> void local_for_all(Function fn) {
#pragma omp parallel for
    for (int i = 0; i < NumBanks; ++i) {
      // Lock should be unnecessary here, but added just in case I missed
      // something...
      lock_bank::mutex_lock(i);
      for (auto &item : m_local_banked_bag[i]) {
        fn(item);
      }
    }
  }

protected:
  size_t m_round_robin;
  ygm::comm<SendBufferManager> m_comm;
  std::vector<std::vector<value_type>> m_local_banked_bag;
  typename ygm::ygm_ptr<self_type> pthis;

  std::atomic<uint32_t> m_next_bank;
};
} // namespace ygm::container::detail
