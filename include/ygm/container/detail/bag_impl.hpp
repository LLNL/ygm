// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once
#include <cereal/archives/json.hpp>
#include <fstream>
#include <vector>
#include <map>
#include <utility>
#include <algorithm>
#include <ygm/comm.hpp>
#include <ygm/detail/std_traits.hpp>
#include <ygm/detail/ygm_ptr.hpp>
#include <ygm/detail/ygm_traits.hpp>
#include <ygm/detail/mpi.hpp>

namespace ygm::container::detail {
template <typename Item, typename Alloc = std::allocator<Item>>
class bag_impl {
 public:
  using value_type = Item;
  using self_type  = bag_impl<Item, Alloc>;

  bag_impl(ygm::comm &comm) : m_comm(comm), pthis(this) { pthis.check(m_comm); }

  ~bag_impl() { m_comm.barrier(); }

  void async_insert(const value_type &item) {
    auto inserter = [](auto mailbox, auto map, const value_type &item) {
      map->m_local_bag.push_back(item);
    };
    int dest = (m_round_robin++ + m_comm.rank()) % m_comm.size();
    m_comm.async(dest, inserter, pthis, item);
  }

  void async_insert(const value_type &item, int dest) {
    auto inserter = [](auto mailbox, auto map, const value_type &item) {
      map->m_local_bag.push_back(item);
    };
    m_comm.async(dest, inserter, pthis, item);
  }

  void async_insert(const std::vector<value_type> item, int dest) {
    auto inserter = [](auto mailbox, auto map, const std::vector<value_type> item) {
      map->m_local_bag.insert(map->m_local_bag.end(), item.begin(), item.end());
    };
    m_comm.async(dest, inserter, pthis, item);
  }
  
  template <typename Function>
  void for_all(Function fn) {
    m_comm.barrier();
    local_for_all(fn);
  }

  value_type local_pop() {
    value_type back_val = m_local_bag.back();
    m_local_bag.pop_back();
    return back_val;
  }

  void clear() {
    m_comm.barrier();
    m_local_bag.clear();
  }

  size_t size() {
    m_comm.barrier();
    return m_comm.all_reduce_sum(m_local_bag.size());
  }

  size_t local_size() {
    return m_local_bag.size();
  }


  void rebalance() {
    m_comm.barrier();

    size_t lsize = local_size();
    size_t* SA = (size_t*)malloc(sizeof(size_t) * m_comm.size());
    MPI_Allgather(&lsize, 1, ygm::detail::mpi_typeof(size_t()),
                  SA, 1, ygm::detail::mpi_typeof(size_t()), 
                  m_comm.get_mpi_comm());
    
    size_t target = size() / m_comm.size() + (m_comm.rank() < size() % m_comm.size());
    size_t* TA = (size_t*)malloc(sizeof(size_t) * m_comm.size());
    MPI_Allgather(&target, 1, ygm::detail::mpi_typeof(size_t()),
                  TA, 1, ygm::detail::mpi_typeof(size_t()),
                  m_comm.get_mpi_comm());

    std::vector< std::pair<int, size_t> > send_vect;

    /* Concurrently iterate through two iterable variables
     *   - c: Cur rank with excess items
     *   - i: Input rank taking items from c
     */
    int c = 0;
    for (int i = 0; i < m_comm.size(); i++) {
      while (SA[i] < TA[i]) { 
        while (SA[c] <= TA[c]) {
          c++; 
        }

        size_t i_needed = TA[i] - SA[i];
        size_t c_excess = SA[c] - TA[c];
        size_t transfer_num = std::min(i_needed, c_excess);

        if (c == m_comm.rank()) {
          send_vect.push_back(std::make_pair(i, transfer_num));
        }

        SA[c] -= transfer_num;
        SA[i] += transfer_num;
      }

      // There is no point in calculating anything higher than personal rank
      if (c > m_comm.rank()) {
        break;
      }
    }

    for (auto it: send_vect) {
      std::vector<value_type> send_vals(it.second);
      for (int i = 0; i < it.second; i++) {
        send_vals[i] = local_pop();
      }
      async_insert(send_vals, it.first);
    }

    m_comm.barrier();
  }

  void swap(self_type &s) {
    m_comm.barrier();
    m_local_bag.swap(s.m_local_bag);
  }

  ygm::comm &comm() { return m_comm; }

  void serialize(const std::string &fname) {
    m_comm.barrier();
    std::string   rank_fname = fname + std::to_string(m_comm.rank());
    std::ofstream os(rank_fname, std::ios::binary);
    cereal::JSONOutputArchive oarchive(os);
    oarchive(m_local_bag, m_round_robin, m_comm.size());
  }

  void deserialize(const std::string &fname) {
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

  template <typename Function>
  void local_for_all(Function fn) {
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

  std::vector<value_type> gather_to_vector(int dest) {
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

  std::vector<value_type> gather_to_vector() {
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

 private:
  template <typename Function>
  void local_for_all_pair_types(Function fn) {
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

 protected:
  size_t                           m_round_robin = 0;
  ygm::comm                        m_comm;
  std::vector<value_type>          m_local_bag;
  typename ygm::ygm_ptr<self_type> pthis;
};
}  // namespace ygm::container::detail
