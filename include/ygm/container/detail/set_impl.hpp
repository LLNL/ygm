// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once
#include <cereal/archives/json.hpp>
#include <fstream>
#include <set>
#include <ygm/comm.hpp>
#include <ygm/container/detail/hash_partitioner.hpp>
#include <ygm/detail/ygm_ptr.hpp>
#include <ygm/detail/ygm_traits.hpp>

namespace ygm::container::detail {
template <typename Key, typename Partitioner = detail::hash_partitioner<Key>,
          typename Compare = std::less<Key>,
          class Alloc      = std::allocator<const Key>>
class set_impl {
 public:
  using self_type = set_impl<Key, Partitioner, Compare, Alloc>;
  using key_type  = Key;

  Partitioner partitioner;

  set_impl(ygm::comm &comm) : m_comm(comm), pthis(this) { pthis.check(m_comm); }

  ~set_impl() { m_comm.barrier(); }

  void async_insert_multi(const key_type &key) {
    auto inserter = [](auto mailbox, auto pset, const key_type &key) {
      pset->m_local_set.insert(key);
    };
    int dest = owner(key);
    m_comm.async(dest, inserter, pthis, key);
  }

  void async_insert_unique(const key_type &key) {
    auto inserter = [](auto mailbox, auto pset, const key_type &key) {
      if (pset->m_local_set.count(key) == 0) {
        pset->m_local_set.insert(key);
      }
    };
    int dest = owner(key);
    m_comm.async(dest, inserter, pthis, key);
  }

  void async_erase(const key_type &key) {
    int  dest          = owner(key);
    auto erase_wrapper = [](auto pcomm, auto pset, const key_type &key) {
      pset->m_local_set.erase(key);
    };

    m_comm.async(dest, erase_wrapper, pthis, key);
  }

  template <typename Visitor, typename... VisitorArgs>
  void async_insert_exe_if_missing(const key_type &key, Visitor visitor,
                                   const VisitorArgs &...args) {
    auto insert_and_visit = [](auto mailbox, auto pset, const key_type &key,
                               const VisitorArgs &...args) {
      if (pset->m_local_set.count(key) == 0) {
        pset->m_local_set.insert(key);
        Visitor *vis = nullptr;
        std::apply(*vis, std::forward_as_tuple(key, args...));
      }
    };
    int dest = owner(key);
    m_comm.async(dest, insert_and_visit, pthis, key,
                 std::forward<const VisitorArgs>(args)...);
  }

  template <typename Visitor, typename... VisitorArgs>
  void async_insert_exe_if_contains(const key_type &key, Visitor visitor,
                                    const VisitorArgs &...args) {
    auto insert_and_visit = [](auto mailbox, auto pset, const key_type &key,
                               const VisitorArgs &...args) {
      if (pset->m_local_set.count(key) == 0) {
        pset->m_local_set.insert(key);
      } else {
        Visitor *vis = nullptr;
        std::apply(*vis, std::forward_as_tuple(key, args...));
      }
    };
    int dest = owner(key);
    m_comm.async(dest, insert_and_visit, pthis, key,
                 std::forward<const VisitorArgs>(args)...);
  }

  template <typename Visitor, typename... VisitorArgs>
  void async_exe_if_missing(const key_type &key, Visitor visitor,
                            const VisitorArgs &...args) {
    auto checker = [](auto mailbox, auto pset, const key_type &key,
                      const VisitorArgs &...args) {
      if (pset->m_local_set.count(key) == 0) {
        Visitor *vis = nullptr;
        std::apply(*vis, std::forward_as_tuple(key, args...));
      }
    };
    int dest = owner(key);
    m_comm.async(dest, checker, pthis, key,
                 std::forward<const VisitorArgs>(args)...);
  }

  template <typename Visitor, typename... VisitorArgs>
  void async_exe_if_contains(const key_type &key, Visitor visitor,
                             const VisitorArgs &...args) {
    auto checker = [](auto mailbox, auto pset, const key_type &key,
                      const VisitorArgs &...args) {
      if (pset->m_local_set.count(key) == 1) {
        Visitor *vis = nullptr;
        std::apply(*vis, std::forward_as_tuple(key, args...));
      }
    };
    int dest = owner(key);
    m_comm.async(dest, checker, pthis, key,
                 std::forward<const VisitorArgs>(args)...);
  }

  template <typename Function>
  void for_all(Function fn) {
    m_comm.barrier();
    local_for_all(fn);
  }

  void clear() {
    m_comm.barrier();
    m_local_set.clear();
  }

  size_t size() {
    m_comm.barrier();
    return m_comm.all_reduce_sum(m_local_set.size());
  }

  size_t count(const key_type &key) {
    m_comm.barrier();
    return m_comm.all_reduce_sum(m_local_set.count(key));
  }

  // Doesn't swap pthis.
  // should we check comm is equal? -- probably
  void swap(self_type &s) {
    m_comm.barrier();
    m_local_set.swap(s.m_local_set);
  }

  typename ygm::ygm_ptr<self_type> get_ygm_ptr() const { return pthis; }

  void serialize(const std::string &fname) {
    m_comm.barrier();
    std::string   rank_fname = fname + std::to_string(m_comm.rank());
    std::ofstream os(rank_fname, std::ios::binary);
    cereal::JSONOutputArchive oarchive(os);
    oarchive(m_local_set, m_comm.size());
  }

  void deserialize(const std::string &fname) {
    m_comm.barrier();

    std::string   rank_fname = fname + std::to_string(m_comm.rank());
    std::ifstream is(rank_fname, std::ios::binary);

    cereal::JSONInputArchive iarchive(is);
    int                      comm_size;
    iarchive(m_local_set, comm_size);

    if (comm_size != m_comm.size()) {
      m_comm.cerr0(
          "Attempting to deserialize set_impl using communicator of "
          "different size than serialized with");
    }
  }

  ygm::comm &comm() { return m_comm; }

  template <typename Function>
  void local_for_all(Function fn) {
    if constexpr (std::is_invocable<decltype(fn), const key_type &>()) {
      std::for_each(m_local_set.begin(), m_local_set.end(), fn);
    } else {
      static_assert(ygm::detail::always_false<>,
                    "local set lambda signature must be invocable with (const "
                    "key_type &) signature");
    }
  }

  int owner(const key_type &key) const {
    auto [owner, rank] = partitioner(key, m_comm.size(), 1024);
    return owner;
  }
  set_impl() = delete;

  std::multiset<key_type, Compare, Alloc> m_local_set;
  ygm::comm                               m_comm;
  typename ygm::ygm_ptr<self_type>        pthis;
};
}  // namespace ygm::container::detail
