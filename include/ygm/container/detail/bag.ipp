// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once

#include <cereal/archives/json.hpp>
#include <ygm/collective.hpp>
#include <ygm/detail/std_traits.hpp>

namespace ygm::container {

template <typename Item, typename Alloc>
bag<Item, Alloc>::bag(ygm::comm &comm) : m_comm(comm), pthis(this) {
  pthis.check(m_comm);
}

template <typename Item, typename Alloc>
bag<Item, Alloc>::~bag() {
  m_comm.barrier();
}

template <typename Item, typename Alloc>
void bag<Item, Alloc>::async_insert(const value_type &item) {
  auto inserter = [](auto mailbox, auto map, const value_type &item) {
    map->m_local_bag.push_back(item);
  };
  int dest = (m_round_robin++ + m_comm.rank()) % m_comm.size();
  m_comm.async(dest, inserter, pthis, item);
}

template <typename Item, typename Alloc>
void bag<Item, Alloc>::async_insert(const value_type &item, int dest) {
  auto inserter = [](auto mailbox, auto map, const value_type &item) {
    map->m_local_bag.push_back(item);
  };
  m_comm.async(dest, inserter, pthis, item);
}

template <typename Item, typename Alloc>
void bag<Item, Alloc>::async_insert(const std::vector<value_type> &items,
                                    int                            dest) {
  auto inserter = [](auto mailbox, auto map,
                     const std::vector<value_type> &item) {
    map->m_local_bag.insert(map->m_local_bag.end(), item.begin(), item.end());
  };
  m_comm.async(dest, inserter, pthis, items);
}

template <typename Item, typename Alloc>
template <typename Function>
void bag<Item, Alloc>::for_all(Function fn) {
  m_comm.barrier();
  local_for_all(fn);
}

template <typename Item, typename Alloc>
void bag<Item, Alloc>::clear() {
  m_comm.barrier();
  m_local_bag.clear();
}

template <typename Item, typename Alloc>
typename bag<Item, Alloc>::size_type bag<Item, Alloc>::size() {
  m_comm.barrier();
  return m_comm.all_reduce_sum(m_local_bag.size());
}

template <typename Item, typename Alloc>
typename bag<Item, Alloc>::size_type bag<Item, Alloc>::local_size() {
  return m_local_bag.size();
}

template <typename Item, typename Alloc>
void bag<Item, Alloc>::rebalance() {
  m_comm.barrier();

  // Find current rank's prefix val and desired target size
  size_t prefix_val  = ygm::prefix_sum(local_size(), m_comm);
  size_t target_size = std::ceil((size() * 1.0) / m_comm.size());

  // Init to_send array where index is dest and value is the num to send
  // int to_send[m_comm.size()] = {0};
  std::unordered_map<size_t, size_t> to_send;

  auto   global_size      = size();
  size_t small_block_size = global_size / m_comm.size();
  size_t large_block_size =
      global_size / m_comm.size() + ((global_size / m_comm.size()) > 0);

  for (size_t i = 0; i < local_size(); i++) {
    size_t idx = prefix_val + i;
    size_t target_rank;

    // Determine target rank to match partitioning in ygm::container::array
    if (idx < (global_size % m_comm.size()) * large_block_size) {
      target_rank = idx / large_block_size;
    } else {
      target_rank = (global_size % m_comm.size()) +
                    (idx - (global_size % m_comm.size()) * large_block_size) /
                        small_block_size;
    }

    if (target_rank != m_comm.rank()) {
      to_send[target_rank]++;
    }
  }
  m_comm.barrier();

  // Build and send bag indexes as calculated by to_send
  for (auto &kv_pair : to_send) {
    async_insert(local_pop(kv_pair.second), kv_pair.first);
  }

  m_comm.barrier();
}

template <typename Item, typename Alloc>
void bag<Item, Alloc>::swap(self_type &s) {
  m_comm.barrier();
  m_local_bag.swap(s.m_local_bag);
}

template <typename Item, typename Alloc>
template <typename RandomFunc>
void bag<Item, Alloc>::local_shuffle(RandomFunc &r) {
  m_comm.barrier();
  std::shuffle(m_local_bag.begin(), m_local_bag.end(), r);
}

template <typename Item, typename Alloc>
void bag<Item, Alloc>::local_shuffle() {
  ygm::default_random_engine<> r(m_comm, std::random_device()());
  local_shuffle(r);
}

template <typename Item, typename Alloc>
template <typename RandomFunc>
void bag<Item, Alloc>::global_shuffle(RandomFunc &r) {
  m_comm.barrier();
  std::vector<value_type> old_local_bag;
  std::swap(old_local_bag, m_local_bag);

  auto send_item = [](auto bag, const value_type &item) {
    bag->m_local_bag.push_back(item);
  };

  std::uniform_int_distribution<> distrib(0, m_comm.size() - 1);
  for (value_type i : old_local_bag) {
    m_comm.async(distrib(r), send_item, pthis, i);
  }
}

template <typename Item, typename Alloc>
void bag<Item, Alloc>::global_shuffle() {
  ygm::default_random_engine<> r(m_comm, std::random_device()());
  global_shuffle(r);
}

template <typename Item, typename Alloc>
ygm::comm &bag<Item, Alloc>::comm() {
  return m_comm;
}

template <typename Item, typename Alloc>
void bag<Item, Alloc>::serialize(const std::string &fname) {
  m_comm.barrier();
  std::string               rank_fname = fname + std::to_string(m_comm.rank());
  std::ofstream             os(rank_fname, std::ios::binary);
  cereal::JSONOutputArchive oarchive(os);
  oarchive(m_local_bag, m_round_robin, m_comm.size());
}

template <typename Item, typename Alloc>
void bag<Item, Alloc>::deserialize(const std::string &fname) {
  m_comm.barrier();

  std::string   rank_fname = fname + std::to_string(m_comm.rank());
  std::ifstream is(rank_fname, std::ios::binary);

  cereal::JSONInputArchive iarchive(is);
  int                      comm_size;
  iarchive(m_local_bag, m_round_robin, comm_size);

  if (comm_size != m_comm.size()) {
    m_comm.cerr0(
        "Attempting to deserialize bag_impl using communicator of "
        "different size than serialized with");
  }
}

template <typename Item, typename Alloc>
template <typename Function>
void bag<Item, Alloc>::local_for_all(Function fn) {
  if constexpr (ygm::detail::is_std_pair<Item>) {
    local_for_all_pair_types(fn);  // pairs get special handling
  } else {
    if constexpr (std::is_invocable<decltype(fn), Item &>()) {
      std::for_each(m_local_bag.begin(), m_local_bag.end(), fn);
    } else {
      static_assert(ygm::detail::always_false<>,
                    "local bag lambdas must be invocable with (value_type &) "
                    "signatures");
    }
  }
}

template <typename Item, typename Alloc>
std::vector<typename bag<Item, Alloc>::value_type>
bag<Item, Alloc>::gather_to_vector(int dest) {
  std::vector<value_type> result;
  auto                    p_res = m_comm.make_ygm_ptr(result);
  m_comm.barrier();
  auto gatherer = [](auto res, const std::vector<value_type> &outer_data) {
    res->insert(res->end(), outer_data.begin(), outer_data.end());
  };
  m_comm.async(dest, gatherer, p_res, m_local_bag);
  m_comm.barrier();
  return result;
}

template <typename Item, typename Alloc>
std::vector<typename bag<Item, Alloc>::value_type>
bag<Item, Alloc>::gather_to_vector() {
  std::vector<value_type> result;
  auto                    p_res = m_comm.make_ygm_ptr(result);
  m_comm.barrier();
  auto result0 = gather_to_vector(0);
  if (m_comm.rank0()) {
    auto distribute = [](auto res, const std::vector<value_type> &data) {
      res->insert(res->end(), data.begin(), data.end());
    };
    m_comm.async_bcast(distribute, p_res, result0);
  }
  m_comm.barrier();
  return result;
}

template <typename Item, typename Alloc>
std::vector<typename bag<Item, Alloc>::value_type> bag<Item, Alloc>::local_pop(
    int n) {
  ASSERT_RELEASE(n <= local_size());

  size_t                  new_size  = local_size() - n;
  auto                    pop_start = m_local_bag.begin() + new_size;
  std::vector<value_type> ret;
  ret.assign(pop_start, m_local_bag.end());
  m_local_bag.resize(new_size);
  return ret;
}

template <typename Item, typename Alloc>
template <typename Function>
void bag<Item, Alloc>::local_for_all_pair_types(Function fn) {
  if constexpr (std::is_invocable<decltype(fn), Item &>()) {
    std::for_each(m_local_bag.begin(), m_local_bag.end(), fn);
  } else if constexpr (std::is_invocable<decltype(fn),
                                         typename Item::first_type &,
                                         typename Item::second_type &>()) {
    for (auto &kv : m_local_bag) {
      fn(kv.first, kv.second);
    }
  } else {
    static_assert(ygm::detail::always_false<>,
                  "local bag<pair> lambdas must be invocable with (pair &) "
                  "or (pair::first_type &, pair::second_type &) signatures");
  }
}

}  // namespace ygm::container
