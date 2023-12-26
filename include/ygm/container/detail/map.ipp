// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once

#include <cereal/archives/json.hpp>
#include <ygm/detail/interrupt_mask.hpp>

namespace ygm::container {

template <typename Key, typename Value, typename Partitioner, typename Compare,
          typename Alloc>
map<Key, Value, Partitioner, Compare, Alloc>::map(ygm::comm &comm)
    : m_default_value{}, m_comm(comm), pthis(this) {
  pthis.check(m_comm);
}

template <typename Key, typename Value, typename Partitioner, typename Compare,
          typename Alloc>
map<Key, Value, Partitioner, Compare, Alloc>::map(ygm::comm         &comm,
                                                  const mapped_type &dv)
    : m_default_value{dv}, m_comm(comm), pthis(this) {
  pthis.check(m_comm);
}

template <typename Key, typename Value, typename Partitioner, typename Compare,
          typename Alloc>
map<Key, Value, Partitioner, Compare, Alloc>::map(const self_type &rhs)
    : m_default_value(rhs.m_default_value), m_comm(rhs.m_comm), pthis(this) {
  m_local_map.insert(std::begin(rhs.m_local_map), std::end(rhs.m_local_map));
  pthis.check(m_comm);
}

template <typename Key, typename Value, typename Partitioner, typename Compare,
          typename Alloc>
map<Key, Value, Partitioner, Compare, Alloc>::~map() {
  m_comm.barrier();
}

template <typename Key, typename Value, typename Partitioner, typename Compare,
          typename Alloc>
void map<Key, Value, Partitioner, Compare, Alloc>::async_insert(
    const key_type &key, const mapped_type &value) {
  auto inserter = [](auto map, const key_type &key, const mapped_type &value) {
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

template <typename Key, typename Value, typename Partitioner, typename Compare,
          typename Alloc>
void map<Key, Value, Partitioner, Compare, Alloc>::async_set(
    const key_type &key, const mapped_type &value) {
  async_insert(key, value);
}

template <typename Key, typename Value, typename Partitioner, typename Compare,
          typename Alloc>
void map<Key, Value, Partitioner, Compare, Alloc>::async_insert_if_missing(
    const key_type &key, const mapped_type &value) {
  async_insert_if_missing_else_visit(key, value,
                                     [](const key_type &k, const mapped_type &v,
                                        const mapped_type &new_value) {});
}

template <typename Key, typename Value, typename Partitioner, typename Compare,
          typename Alloc>
template <typename Visitor, typename... VisitorArgs>
void map<Key, Value, Partitioner, Compare, Alloc>::async_visit(
    const key_type &key, Visitor visitor, const VisitorArgs &...args) {
  int  dest          = owner(key);
  auto visit_wrapper = [](auto pmap, const key_type &key,
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

template <typename Key, typename Value, typename Partitioner, typename Compare,
          typename Alloc>
template <typename Visitor, typename... VisitorArgs>
void map<Key, Value, Partitioner, Compare, Alloc>::async_visit_if_exists(
    const key_type &key, Visitor visitor, const VisitorArgs &...args) {
  int  dest          = owner(key);
  auto visit_wrapper = [](auto pmap, const key_type &key,
                          const VisitorArgs &...args) {
    Visitor *vis = nullptr;
    pmap->local_visit(key, *vis, args...);
  };

  m_comm.async(dest, visit_wrapper, pthis, key,
               std::forward<const VisitorArgs>(args)...);
}

template <typename Key, typename Value, typename Partitioner, typename Compare,
          typename Alloc>
template <typename Visitor, typename... VisitorArgs>
void map<Key, Value, Partitioner, Compare,
         Alloc>::async_insert_if_missing_else_visit(const key_type    &key,
                                                    const mapped_type &value,
                                                    Visitor            visitor,
                                                    const VisitorArgs
                                                        &...args) {
  int  dest                      = owner(key);
  auto insert_else_visit_wrapper = [](auto pmap, const key_type &key,
                                      const mapped_type &value,
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

template <typename Key, typename Value, typename Partitioner, typename Compare,
          typename Alloc>
template <typename ReductionOp>
void map<Key, Value, Partitioner, Compare, Alloc>::async_reduce(
    const key_type &key, const mapped_type &value, ReductionOp reducer) {
  int  dest           = owner(key);
  auto reduce_wrapper = [](auto pmap, const key_type &key,
                           const mapped_type &value) {
    auto itr = pmap->m_local_map.find(key);
    if (itr == pmap->m_local_map.end()) {
      pmap->m_local_map.insert(std::make_pair(key, value));
    } else {
      ReductionOp *reducer = nullptr;
      itr->second          = (*reducer)(itr->second, value);
    }
  };

  m_comm.async(dest, reduce_wrapper, pthis, key, value);
}

template <typename Key, typename Value, typename Partitioner, typename Compare,
          typename Alloc>
void map<Key, Value, Partitioner, Compare, Alloc>::async_erase(
    const key_type &key) {
  int  dest          = owner(key);
  auto erase_wrapper = [](auto pmap, const key_type &key) {
    pmap->local_erase(key);
  };

  m_comm.async(dest, erase_wrapper, pthis, key);
}

template <typename Key, typename Value, typename Partitioner, typename Compare,
          typename Alloc>
template <typename Function>
void map<Key, Value, Partitioner, Compare, Alloc>::for_all(Function fn) {
  m_comm.barrier();
  local_for_all(fn);
}

template <typename Key, typename Value, typename Partitioner, typename Compare,
          typename Alloc>
void map<Key, Value, Partitioner, Compare, Alloc>::clear() {
  m_comm.barrier();
  m_local_map.clear();
}

template <typename Key, typename Value, typename Partitioner, typename Compare,
          typename Alloc>
typename map<Key, Value, Partitioner, Compare, Alloc>::size_type
map<Key, Value, Partitioner, Compare, Alloc>::size() {
  m_comm.barrier();
  return m_comm.all_reduce_sum(m_local_map.size());
}

template <typename Key, typename Value, typename Partitioner, typename Compare,
          typename Alloc>
typename map<Key, Value, Partitioner, Compare, Alloc>::size_type
map<Key, Value, Partitioner, Compare, Alloc>::count(const key_type &key) {
  m_comm.barrier();
  return m_comm.all_reduce_sum(m_local_map.count(key));
}

// Doesn't swap pthis.
// should we check comm is equal? -- probably
template <typename Key, typename Value, typename Partitioner, typename Compare,
          typename Alloc>
void map<Key, Value, Partitioner, Compare, Alloc>::swap(self_type &s) {
  m_comm.barrier();
  std::swap(m_default_value, s.m_default_value);
  m_local_map.swap(s.m_local_map);
}

template <typename Key, typename Value, typename Partitioner, typename Compare,
          typename Alloc>
template <typename STLKeyContainer>
std::map<typename map<Key, Value, Partitioner, Compare, Alloc>::key_type,
         typename map<Key, Value, Partitioner, Compare, Alloc>::mapped_type>
map<Key, Value, Partitioner, Compare, Alloc>::all_gather(
    const STLKeyContainer &keys) {
  std::map<key_type, mapped_type>               to_return;
  ygm::ygm_ptr<std::map<key_type, mapped_type>> preturn(&to_return);

  auto fetcher = [](int from, const key_type &key, auto pmap, auto pcont) {
    auto returner = [](const key_type                 &key,
                       const std::vector<mapped_type> &values, auto pcont) {
      for (const auto &v : values) {
        pcont->insert(std::make_pair(key, v));
      }
    };
    auto values = pmap->local_get(key);
    pmap->comm().async(from, returner, key, values, pcont);
  };

  m_comm.barrier();
  for (const auto &key : keys) {
    int o = owner(key);
    m_comm.async(o, fetcher, m_comm.rank(), key, pthis, preturn);
  }
  m_comm.barrier();

  return to_return;
}

template <typename Key, typename Value, typename Partitioner, typename Compare,
          typename Alloc>
typename map<Key, Value, Partitioner, Compare, Alloc>::ptr_type
map<Key, Value, Partitioner, Compare, Alloc>::get_ygm_ptr() const {
  return pthis;
}

template <typename Key, typename Value, typename Partitioner, typename Compare,
          typename Alloc>
void map<Key, Value, Partitioner, Compare, Alloc>::serialize(
    const std::string &fname) {
  m_comm.barrier();
  std::string               rank_fname = fname + std::to_string(m_comm.rank());
  std::ofstream             os(rank_fname, std::ios::binary);
  cereal::JSONOutputArchive oarchive(os);
  oarchive(m_local_map, m_default_value, m_comm.size());
}

template <typename Key, typename Value, typename Partitioner, typename Compare,
          typename Alloc>
void map<Key, Value, Partitioner, Compare, Alloc>::deserialize(
    const std::string &fname) {
  m_comm.barrier();

  std::string   rank_fname = fname + std::to_string(m_comm.rank());
  std::ifstream is(rank_fname, std::ios::binary);

  cereal::JSONInputArchive iarchive(is);
  int                      comm_size;
  iarchive(m_local_map, m_default_value, comm_size);

  if (comm_size != m_comm.size()) {
    m_comm.cerr0(
        "Attempting to deserialize map using communicator of "
        "different size than serialized with");
  }
}

template <typename Key, typename Value, typename Partitioner, typename Compare,
          typename Alloc>
int map<Key, Value, Partitioner, Compare, Alloc>::owner(
    const key_type &key) const {
  auto [owner, rank] = partitioner(key, m_comm.size(), 1024);
  return owner;
}

template <typename Key, typename Value, typename Partitioner, typename Compare,
          typename Alloc>
bool map<Key, Value, Partitioner, Compare, Alloc>::is_mine(
    const key_type &key) const {
  return owner(key) == m_comm.rank();
}

template <typename Key, typename Value, typename Partitioner, typename Compare,
          typename Alloc>
std::vector<typename map<Key, Value, Partitioner, Compare, Alloc>::mapped_type>
map<Key, Value, Partitioner, Compare, Alloc>::local_get(const key_type &key) {
  std::vector<mapped_type> to_return;

  auto range = m_local_map.equal_range(key);
  for (auto itr = range.first; itr != range.second; ++itr) {
    to_return.push_back(itr->second);
  }

  return to_return;
}

template <typename Key, typename Value, typename Partitioner, typename Compare,
          typename Alloc>
template <typename Function, typename... VisitorArgs>
void map<Key, Value, Partitioner, Compare, Alloc>::local_visit(
    const key_type &key, Function &fn, const VisitorArgs &...args) {
  ygm::detail::interrupt_mask mask(m_comm);

  auto range = m_local_map.equal_range(key);
  if constexpr (std::is_invocable<decltype(fn), const key_type &, mapped_type &,
                                  VisitorArgs &...>() ||
                std::is_invocable<decltype(fn), ptr_type, const key_type &,
                                  mapped_type &, VisitorArgs &...>()) {
    for (auto itr = range.first; itr != range.second; ++itr) {
      ygm::meta::apply_optional(
          fn, std::make_tuple(pthis),
          std::forward_as_tuple(itr->first, itr->second, args...));
    }
  } else {
    static_assert(ygm::detail::always_false<>,
                  "remote map lambda signature must be invocable with (const "
                  "&key_type, mapped_type&, ...) or (ptr_type, const "
                  "&key_type, mapped_type&, ...) signatures");
  }
}

template <typename Key, typename Value, typename Partitioner, typename Compare,
          typename Alloc>
void map<Key, Value, Partitioner, Compare, Alloc>::local_erase(
    const key_type &key) {
  m_local_map.erase(key);
}

template <typename Key, typename Value, typename Partitioner, typename Compare,
          typename Alloc>
ygm::comm &map<Key, Value, Partitioner, Compare, Alloc>::comm() {
  return m_comm;
}

template <typename Key, typename Value, typename Partitioner, typename Compare,
          typename Alloc>
template <typename Function>
void map<Key, Value, Partitioner, Compare, Alloc>::local_for_all(Function fn) {
  if constexpr (std::is_invocable<decltype(fn), const key_type,
                                  mapped_type &>()) {
    for (std::pair<const key_type, mapped_type> &kv : m_local_map) {
      fn(kv.first, kv.second);
    }
  } else {
    static_assert(ygm::detail::always_false<>,
                  "local map lambda signature must be invocable with (const "
                  "&key_type, mapped_type&) signature");
  }
}

template <typename Key, typename Value, typename Partitioner, typename Compare,
          typename Alloc>
template <typename CompareFunction>
std::vector<std::pair<
    typename map<Key, Value, Partitioner, Compare, Alloc>::key_type,
    typename map<Key, Value, Partitioner, Compare, Alloc>::mapped_type>>
map<Key, Value, Partitioner, Compare, Alloc>::topk(size_t          k,
                                                   CompareFunction cfn) {
  using vec_type = std::vector<std::pair<key_type, mapped_type>>;

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

template <typename Key, typename Value, typename Partitioner, typename Compare,
          typename Alloc>
const typename map<Key, Value, Partitioner, Compare, Alloc>::mapped_type &
map<Key, Value, Partitioner, Compare, Alloc>::default_value() const {
  return m_default_value;
}

template <typename Key, typename Value, typename Partitioner, typename Compare,
          typename Alloc>
multimap<Key, Value, Partitioner, Compare, Alloc>::multimap(ygm::comm &comm)
    : m_default_value{}, m_comm(comm), pthis(this) {
  pthis.check(m_comm);
}

template <typename Key, typename Value, typename Partitioner, typename Compare,
          typename Alloc>
multimap<Key, Value, Partitioner, Compare, Alloc>::multimap(
    ygm::comm &comm, const mapped_type &dv)
    : m_default_value{dv}, m_comm(comm), pthis(this) {
  pthis.check(m_comm);
}

template <typename Key, typename Value, typename Partitioner, typename Compare,
          typename Alloc>
multimap<Key, Value, Partitioner, Compare, Alloc>::multimap(
    const self_type &rhs)
    : m_default_value(rhs.m_default_value), m_comm(rhs.m_comm), pthis(this) {
  m_local_map.insert(std::begin(rhs.m_local_map), std::end(rhs.m_local_map));
  pthis.check(m_comm);
}

template <typename Key, typename Value, typename Partitioner, typename Compare,
          typename Alloc>
multimap<Key, Value, Partitioner, Compare, Alloc>::~multimap() {
  m_comm.barrier();
}

template <typename Key, typename Value, typename Partitioner, typename Compare,
          typename Alloc>
void multimap<Key, Value, Partitioner, Compare, Alloc>::async_insert(
    const key_type &key, const mapped_type &value) {
  auto inserter = [](auto mailbox, auto map, const key_type &key,
                     const mapped_type &value) {
    map->m_local_map.insert(std::make_pair(key, value));
  };
  int dest = owner(key);
  m_comm.async(dest, inserter, pthis, key, value);
}

template <typename Key, typename Value, typename Partitioner, typename Compare,
          typename Alloc>
void multimap<Key, Value, Partitioner, Compare, Alloc>::async_insert(
    const std::pair<key_type, mapped_type> &kv) {
  async_insert(kv.first, kv.second);
}

template <typename Key, typename Value, typename Partitioner, typename Compare,
          typename Alloc>
void multimap<Key, Value, Partitioner, Compare, Alloc>::async_set(
    const key_type &key, const mapped_type &value) {
  async_insert(key, value);
}

template <typename Key, typename Value, typename Partitioner, typename Compare,
          typename Alloc>
void multimap<Key, Value, Partitioner, Compare, Alloc>::async_insert_if_missing(
    const key_type &key, const mapped_type &value) {
  async_insert_if_missing_else_visit(key, value,
                                     [](const key_type &k, const mapped_type &v,
                                        const mapped_type &new_value) {});
}

template <typename Key, typename Value, typename Partitioner, typename Compare,
          typename Alloc>
template <typename Visitor, typename... VisitorArgs>
void multimap<Key, Value, Partitioner, Compare, Alloc>::async_visit(
    const key_type &key, Visitor visitor, const VisitorArgs &...args) {
  int  dest          = owner(key);
  auto visit_wrapper = [](auto pmap, const key_type &key,
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

template <typename Key, typename Value, typename Partitioner, typename Compare,
          typename Alloc>
template <typename Visitor, typename... VisitorArgs>
void multimap<Key, Value, Partitioner, Compare, Alloc>::async_visit_group(
    const key_type &key, Visitor visitor, const VisitorArgs &...args) {
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

template <typename Key, typename Value, typename Partitioner, typename Compare,
          typename Alloc>
template <typename Visitor, typename... VisitorArgs>
void multimap<Key, Value, Partitioner, Compare, Alloc>::async_visit_if_exists(
    const key_type &key, Visitor visitor, const VisitorArgs &...args) {
  int  dest          = owner(key);
  auto visit_wrapper = [](auto pmap, const key_type &key,
                          const VisitorArgs &...args) {
    Visitor *vis = nullptr;
    pmap->local_visit(key, *vis, args...);
  };

  m_comm.async(dest, visit_wrapper, pthis, key,
               std::forward<const VisitorArgs>(args)...);
}

template <typename Key, typename Value, typename Partitioner, typename Compare,
          typename Alloc>
template <typename Visitor, typename... VisitorArgs>
void multimap<Key, Value, Partitioner, Compare, Alloc>::
    async_insert_if_missing_else_visit(const key_type    &key,
                                       const mapped_type &value,
                                       Visitor            visitor,
                                       const VisitorArgs &...args) {
  int  dest                      = owner(key);
  auto insert_else_visit_wrapper = [](auto pmap, const key_type &key,
                                      const mapped_type &value,
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

template <typename Key, typename Value, typename Partitioner, typename Compare,
          typename Alloc>
template <typename ReductionOp>
void multimap<Key, Value, Partitioner, Compare, Alloc>::async_reduce(
    const key_type &key, const mapped_type &value, ReductionOp reducer) {
  int  dest           = owner(key);
  auto reduce_wrapper = [](auto pmap, const key_type &key,
                           const mapped_type &value) {
    auto itr = pmap->m_local_map.find(key);
    if (itr == pmap->m_local_map.end()) {
      pmap->m_local_map.insert(std::make_pair(key, value));
    } else {
      ReductionOp *reducer = nullptr;
      itr->second          = (*reducer)(itr->second, value);
    }
  };

  m_comm.async(dest, reduce_wrapper, pthis, key, value);
}

template <typename Key, typename Value, typename Partitioner, typename Compare,
          typename Alloc>
void multimap<Key, Value, Partitioner, Compare, Alloc>::async_erase(
    const key_type &key) {
  int  dest          = owner(key);
  auto erase_wrapper = [](auto pmap, const key_type &key) {
    pmap->local_erase(key);
  };

  m_comm.async(dest, erase_wrapper, pthis, key);
}

template <typename Key, typename Value, typename Partitioner, typename Compare,
          typename Alloc>
template <typename Function>
void multimap<Key, Value, Partitioner, Compare, Alloc>::for_all(Function fn) {
  m_comm.barrier();
  local_for_all(fn);
}

template <typename Key, typename Value, typename Partitioner, typename Compare,
          typename Alloc>
void multimap<Key, Value, Partitioner, Compare, Alloc>::clear() {
  m_comm.barrier();
  m_local_map.clear();
}

template <typename Key, typename Value, typename Partitioner, typename Compare,
          typename Alloc>
typename multimap<Key, Value, Partitioner, Compare, Alloc>::size_type
multimap<Key, Value, Partitioner, Compare, Alloc>::size() {
  m_comm.barrier();
  return m_comm.all_reduce_sum(m_local_map.size());
}

template <typename Key, typename Value, typename Partitioner, typename Compare,
          typename Alloc>
typename multimap<Key, Value, Partitioner, Compare, Alloc>::size_type
multimap<Key, Value, Partitioner, Compare, Alloc>::count(const key_type &key) {
  m_comm.barrier();
  return m_comm.all_reduce_sum(m_local_map.count(key));
}

// Doesn't swap pthis.
// should we check comm is equal? -- probably
template <typename Key, typename Value, typename Partitioner, typename Compare,
          typename Alloc>
void multimap<Key, Value, Partitioner, Compare, Alloc>::swap(self_type &s) {
  m_comm.barrier();
  std::swap(m_default_value, s.m_default_value);
  m_local_map.swap(s.m_local_map);
}

template <typename Key, typename Value, typename Partitioner, typename Compare,
          typename Alloc>
template <typename STLKeyContainer>
std::multimap<
    typename multimap<Key, Value, Partitioner, Compare, Alloc>::key_type,
    typename multimap<Key, Value, Partitioner, Compare, Alloc>::mapped_type>
multimap<Key, Value, Partitioner, Compare, Alloc>::all_gather(
    const STLKeyContainer &keys) {
  std::multimap<key_type, mapped_type>               to_return;
  ygm::ygm_ptr<std::multimap<key_type, mapped_type>> preturn(&to_return);

  auto fetcher = [](int from, const key_type &key, auto pmap, auto pcont) {
    auto returner = [](const key_type                 &key,
                       const std::vector<mapped_type> &values, auto pcont) {
      for (const auto &v : values) {
        pcont->insert(std::make_pair(key, v));
      }
    };
    auto values = pmap->local_get(key);
    pmap->comm().async(from, returner, key, values, pcont);
  };

  m_comm.barrier();
  for (const auto &key : keys) {
    int o = owner(key);
    m_comm.async(o, fetcher, m_comm.rank(), key, pthis, preturn);
  }
  m_comm.barrier();

  return to_return;
}

template <typename Key, typename Value, typename Partitioner, typename Compare,
          typename Alloc>
typename multimap<Key, Value, Partitioner, Compare, Alloc>::ptr_type
multimap<Key, Value, Partitioner, Compare, Alloc>::get_ygm_ptr() const {
  return pthis;
}

template <typename Key, typename Value, typename Partitioner, typename Compare,
          typename Alloc>
void multimap<Key, Value, Partitioner, Compare, Alloc>::serialize(
    const std::string &fname) {
  m_comm.barrier();
  std::string               rank_fname = fname + std::to_string(m_comm.rank());
  std::ofstream             os(rank_fname, std::ios::binary);
  cereal::JSONOutputArchive oarchive(os);
  oarchive(m_local_map, m_default_value, m_comm.size());
}

template <typename Key, typename Value, typename Partitioner, typename Compare,
          typename Alloc>
void multimap<Key, Value, Partitioner, Compare, Alloc>::deserialize(
    const std::string &fname) {
  m_comm.barrier();

  std::string   rank_fname = fname + std::to_string(m_comm.rank());
  std::ifstream is(rank_fname, std::ios::binary);

  cereal::JSONInputArchive iarchive(is);
  int                      comm_size;
  iarchive(m_local_map, m_default_value, comm_size);

  if (comm_size != m_comm.size()) {
    m_comm.cerr0(
        "Attempting to deserialize map using communicator of "
        "different size than serialized with");
  }
}

template <typename Key, typename Value, typename Partitioner, typename Compare,
          typename Alloc>
int multimap<Key, Value, Partitioner, Compare, Alloc>::owner(
    const key_type &key) const {
  auto [owner, rank] = partitioner(key, m_comm.size(), 1024);
  return owner;
}

template <typename Key, typename Value, typename Partitioner, typename Compare,
          typename Alloc>
bool multimap<Key, Value, Partitioner, Compare, Alloc>::is_mine(
    const key_type &key) const {
  return owner(key) == m_comm.rank();
}

template <typename Key, typename Value, typename Partitioner, typename Compare,
          typename Alloc>
std::vector<
    typename multimap<Key, Value, Partitioner, Compare, Alloc>::mapped_type>
multimap<Key, Value, Partitioner, Compare, Alloc>::local_get(
    const key_type &key) {
  std::vector<mapped_type> to_return;

  auto range = m_local_map.equal_range(key);
  for (auto itr = range.first; itr != range.second; ++itr) {
    to_return.push_back(itr->second);
  }

  return to_return;
}

template <typename Key, typename Value, typename Partitioner, typename Compare,
          typename Alloc>
template <typename Function, typename... VisitorArgs>
void multimap<Key, Value, Partitioner, Compare, Alloc>::local_visit(
    const key_type &key, Function &fn, const VisitorArgs &...args) {
  ygm::detail::interrupt_mask mask(m_comm);

  auto range = m_local_map.equal_range(key);
  if constexpr (std::is_invocable<decltype(fn), const key_type &, mapped_type &,
                                  VisitorArgs &...>() ||
                std::is_invocable<decltype(fn), ptr_type, const key_type &,
                                  mapped_type &, VisitorArgs &...>()) {
    for (auto itr = range.first; itr != range.second; ++itr) {
      ygm::meta::apply_optional(
          fn, std::make_tuple(pthis),
          std::forward_as_tuple(itr->first, itr->second, args...));
    }
  } else {
    static_assert(ygm::detail::always_false<>,
                  "remote map lambda signature must be invocable with (const "
                  "&key_type, mapped_type&, ...) or (ptr_type, const "
                  "&key_type, mapped_type&, ...) signatures");
  }
}

template <typename Key, typename Value, typename Partitioner, typename Compare,
          typename Alloc>
void multimap<Key, Value, Partitioner, Compare, Alloc>::local_erase(
    const key_type &key) {
  m_local_map.erase(key);
}

template <typename Key, typename Value, typename Partitioner, typename Compare,
          typename Alloc>
ygm::comm &multimap<Key, Value, Partitioner, Compare, Alloc>::comm() {
  return m_comm;
}

template <typename Key, typename Value, typename Partitioner, typename Compare,
          typename Alloc>
template <typename Function>
void multimap<Key, Value, Partitioner, Compare, Alloc>::local_for_all(
    Function fn) {
  if constexpr (std::is_invocable<decltype(fn), const key_type,
                                  mapped_type &>()) {
    for (std::pair<const key_type, mapped_type> &kv : m_local_map) {
      fn(kv.first, kv.second);
    }
  } else {
    static_assert(ygm::detail::always_false<>,
                  "local map lambda signature must be invocable with (const "
                  "&key_type, mapped_type&) signature");
  }
}

template <typename Key, typename Value, typename Partitioner, typename Compare,
          typename Alloc>
template <typename CompareFunction>
std::vector<std::pair<
    typename multimap<Key, Value, Partitioner, Compare, Alloc>::key_type,
    typename multimap<Key, Value, Partitioner, Compare, Alloc>::mapped_type>>
multimap<Key, Value, Partitioner, Compare, Alloc>::topk(size_t          k,
                                                        CompareFunction cfn) {
  using vec_type = std::vector<std::pair<key_type, mapped_type>>;

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

template <typename Key, typename Value, typename Partitioner, typename Compare,
          typename Alloc>
const typename multimap<Key, Value, Partitioner, Compare, Alloc>::mapped_type &
multimap<Key, Value, Partitioner, Compare, Alloc>::default_value() const {
  return m_default_value;
}

}  // namespace ygm::container
