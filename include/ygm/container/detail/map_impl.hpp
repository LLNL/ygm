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
#include <ygm/detail/interrupt_mask.hpp>
#include <ygm/detail/ygm_ptr.hpp>

namespace ygm::container::detail {

template <class...>
constexpr std::false_type always_false{};

template <typename Key, typename Value,
          typename Partitioner = detail::hash_partitioner<Key>,
          typename Compare     = std::less<Key>,
          class Alloc          = std::allocator<std::pair<const Key, Value>>>
class map_impl {
 public:
  using self_type  = map_impl<Key, Value, Partitioner, Compare, Alloc>;
  using ptr_type   = typename ygm::ygm_ptr<self_type>;
  using value_type = Value;
  using key_type   = Key;

  Partitioner partitioner;

  map_impl(ygm::comm &comm) : m_default_value{}, m_comm(comm), pthis(this) {
    pthis.check(m_comm);
  }

  map_impl(ygm::comm &comm, const value_type &dv)
      : m_default_value(dv), m_comm(comm), pthis(this) {
    pthis.check(m_comm);
  }

  map_impl(const self_type &rhs)
      : m_default_value(rhs.m_default_value), m_comm(rhs.m_comm), pthis(this) {
    m_local_map.insert(std::begin(rhs.m_local_map), std::end(rhs.m_local_map));
    pthis.check(m_comm);
  }

  ~map_impl() { m_comm.barrier(); }

  void async_insert_unique(const key_type &key, const value_type &value) {
    auto inserter = [](auto mailbox, auto map, const key_type &key,
                       const value_type &value) {
      auto itr = map->m_local_map.find(key);
      if (itr != map->m_local_map.end()) {
        itr->second = value;
      } else {
        map->m_local_map.insert(std::make_pair(key, value));
      }
    };
    int dest = owner(key);
    m_comm.async(dest, inserter, pthis, key, value);
  }

  void async_insert_if_missing(const key_type &key, const value_type &value) {
    async_insert_if_missing_else_visit(
        key, value,
        [](const std::pair<key_type, value_type> &kv,
           const value_type                      &new_value) {});
  }

  void async_insert_multi(const key_type &key, const value_type &value) {
    auto inserter = [](auto mailbox, auto map, const key_type &key,
                       const value_type &value) {
      map->m_local_map.insert(std::make_pair(key, value));
    };
    int dest = owner(key);
    m_comm.async(dest, inserter, pthis, key, value);
  }

  template <typename Visitor, typename... VisitorArgs>
  void async_visit(const key_type &key, Visitor visitor,
                   const VisitorArgs &...args) {
    int  dest          = owner(key);
    auto visit_wrapper = [](auto pcomm, auto pmap, const key_type &key,
                            const VisitorArgs &...args) {
      auto range = pmap->m_local_map.equal_range(key);
      if (range.first == range.second) {  // check if not in range
        pmap->m_local_map.insert(std::make_pair(key, pmap->m_default_value));
        range = pmap->m_local_map.equal_range(key);
        ASSERT_DEBUG(range.first != range.second);
      }
      Visitor *vis = nullptr;
      pmap->local_visit(key, *vis, args...);
    };

    m_comm.async(dest, visit_wrapper, pthis, key,
                 std::forward<const VisitorArgs>(args)...);
  }

  template <typename Visitor, typename... VisitorArgs>
  void async_visit_group(const key_type &key, Visitor visitor,
                         const VisitorArgs &...args) {
    int  dest          = owner(key);
    auto visit_wrapper = [](auto pcomm, auto pmap, const key_type &key,
                            const VisitorArgs &...args) {
      auto range = pmap->m_local_map.equal_range(key);
      if (range.first == range.second) {  // check if not in range
        pmap->m_local_map.insert(std::make_pair(key, pmap->m_default_value));
        range = pmap->m_local_map.equal_range(key);
        ASSERT_DEBUG(range.first != range.second);
      }

      ygm::detail::interrupt_mask mask(pmap->m_comm);

      Visitor *vis = nullptr;
      ygm::meta::apply_optional(
          *vis, std::make_tuple(pmap),
          std::forward_as_tuple(range.first, range.second, args...));
    };

    m_comm.async(dest, visit_wrapper, pthis, key,
                 std::forward<const VisitorArgs>(args)...);
  }

  template <typename Visitor, typename... VisitorArgs>
  void async_visit_if_exists(const key_type &key, Visitor visitor,
                             const VisitorArgs &...args) {
    int  dest          = owner(key);
    auto visit_wrapper = [](auto pcomm, auto pmap, const key_type &key,
                            const VisitorArgs &...args) {
      Visitor *vis = nullptr;
      pmap->local_visit(key, *vis, args...);
    };

    m_comm.async(dest, visit_wrapper, pthis, key,
                 std::forward<const VisitorArgs>(args)...);
  }

  template <typename Visitor, typename... VisitorArgs>
  void async_insert_if_missing_else_visit(const key_type   &key,
                                          const value_type &value,
                                          Visitor           visitor,
                                          const VisitorArgs &...args) {
    int  dest                      = owner(key);
    auto insert_else_visit_wrapper = [](auto pmap, const key_type &key,
                                        const value_type &value,
                                        const VisitorArgs &...args) {
      auto itr = pmap->m_local_map.find(key);
      if (itr == pmap->m_local_map.end()) {
        pmap->m_local_map.insert(std::make_pair(key, value));
      } else {
        Visitor *vis = nullptr;
        pmap->local_visit(key, *vis, value, args...);
      }
    };

    m_comm.async(dest, insert_else_visit_wrapper, pthis, key, value,
                 std::forward<const VisitorArgs>(args)...);
  }

  void async_erase(const key_type &key) {
    int  dest          = owner(key);
    auto erase_wrapper = [](auto pcomm, auto pmap, const key_type &key) {
      pmap->local_erase(key);
    };

    m_comm.async(dest, erase_wrapper, pthis, key);
  }

  size_t local_count(const key_type &key) { return m_local_map.count(key); }

  template <typename Function>
  void for_all(Function fn) {
    m_comm.barrier();
    local_for_all(fn);
  }

  void clear() {
    m_comm.barrier();
    m_local_map.clear();
  }

  size_t size() {
    m_comm.barrier();
    return m_comm.all_reduce_sum(m_local_map.size());
  }

  size_t count(const key_type &key) {
    m_comm.barrier();
    return m_comm.all_reduce_sum(m_local_map.count(key));
  }

  // Doesn't swap pthis.
  // should we check comm is equal? -- probably
  void swap(self_type &s) {
    m_comm.barrier();
    std::swap(m_default_value, s.m_default_value);
    m_local_map.swap(s.m_local_map);
  }

  template <typename STLKeyContainer, typename MapKeyValue>
  void all_gather(const STLKeyContainer &keys, MapKeyValue &output) {
    ygm::ygm_ptr<MapKeyValue> preturn(&output);

    auto fetcher = [](auto pcomm, int from, const key_type &key, auto pmap,
                      auto pcont) {
      auto returner = [](auto pcomm, const key_type &key,
                         const std::vector<value_type> &values, auto pcont) {
        for (const auto &v : values) {
          pcont->insert(std::make_pair(key, v));
        }
      };
      auto values = pmap->local_get(key);
      pcomm->async(from, returner, key, values, pcont);
    };

    m_comm.barrier();
    for (const auto &key : keys) {
      int o = owner(key);
      m_comm.async(o, fetcher, m_comm.rank(), key, pthis, preturn);
    }
    m_comm.barrier();
  }

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

  int owner(const key_type &key) const {
    auto [owner, rank] = partitioner(key, m_comm.size(), 1024);
    return owner;
  }

  bool is_mine(const key_type &key) const {
    return owner(key) == m_comm.rank();
  }

  std::vector<value_type> local_get(const key_type &key) {
    std::vector<value_type> to_return;

    auto range = m_local_map.equal_range(key);
    for (auto itr = range.first; itr != range.second; ++itr) {
      to_return.push_back(itr->second);
    }

    return to_return;
  }

  template <typename Function, typename... VisitorArgs>
  void local_visit(const key_type &key, Function &fn,
                   const VisitorArgs &...args) {
    ygm::detail::interrupt_mask mask(m_comm);

    auto range = m_local_map.equal_range(key);
    // the order of conditionals matters
    // Legacy lambdas with optional map pointer arguments and no visitor
    // arguments must specify that the second argument is a pair. (auto pmap,
    // auto kv_pair) will throw a compiler error.
    if constexpr (std::is_invocable<decltype(fn), const key_type &,
                                    value_type &, VisitorArgs &...>() ||
                  std::is_invocable<decltype(fn), ptr_type, const key_type &,
                                    value_type &, VisitorArgs &...>()) {
      for (auto itr = range.first; itr != range.second; ++itr) {
        ygm::meta::apply_optional(
            fn, std::make_tuple(pthis),
            std::forward_as_tuple(itr->first, itr->second, args...));
      }
    } else if constexpr (
        std::is_invocable<decltype(fn), std::pair<const key_type, value_type> &,
                          VisitorArgs &...>() ||
        std::is_invocable<decltype(fn), ptr_type,
                          std::pair<const key_type, value_type> &,
                          VisitorArgs &...>()) {
      for (auto itr = range.first; itr != range.second; ++itr) {
        ygm::meta::apply_optional(fn, std::make_tuple(pthis),
                                  std::forward_as_tuple(*itr, args...));
      }
    } else {
      static_assert(always_false<>);  // check your lambda signatures!
    }
  }

  void local_erase(const key_type &key) { m_local_map.erase(key); }

  void local_clear() { m_local_map.clear(); }

  size_t local_size() const { return m_local_map.size(); }

  size_t local_const(const key_type &k) const { return m_local_map.count(k); }

  ygm::comm &comm() { return m_comm; }

  template <typename Function>
  void local_for_all(Function fn) {
    if constexpr (std::is_invocable<decltype(fn), const key_type,
                                    value_type &>()) {
      for (std::pair<const key_type, value_type> &kv : m_local_map) {
        fn(kv.first, kv.second);
      }
    } else if constexpr (std::is_invocable<
                             decltype(fn),
                             std::pair<const key_type, value_type &>>()) {
      for (std::pair<const key_type, value_type> &kv : m_local_map) {
        fn(kv);
      }
      // std::for_each(m_local_map.begin(), m_local_map.end(), fn);
    } else {
      static_assert(always_false<>);  // check your lambda signatures!
    }
  }

  template <typename CompareFunction>
  std::vector<std::pair<key_type, value_type>> topk(size_t          k,
                                                    CompareFunction cfn) {
    using vec_type = std::vector<std::pair<key_type, value_type>>;

    m_comm.barrier();

    vec_type local_topk;
    for (const auto &kv : m_local_map) {
      local_topk.push_back(kv);
      std::sort(local_topk.begin(), local_topk.end(), cfn);
      if (local_topk.size() > k) {
        local_topk.pop_back();
      }
    }

    auto to_return = m_comm.all_reduce(
        local_topk, [cfn, k](const vec_type &va, const vec_type &vb) {
          vec_type out(va.begin(), va.end());
          out.insert(out.end(), vb.begin(), vb.end());
          std::sort(out.begin(), out.end(), cfn);
          while (out.size() > k) {
            out.pop_back();
          }
          return out;
        });
    return to_return;
  }

  const value_type &default_value() const { return m_default_value; }

 protected:
  map_impl() = delete;

  value_type                                          m_default_value;
  std::multimap<key_type, value_type, Compare, Alloc> m_local_map;
  ygm::comm                                           m_comm;
  ptr_type                                            pthis;
};
}  // namespace ygm::container::detail
