// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once
#include <cereal/archives/json.hpp>
#include <fstream>
#include <vector>
#include <ygm/comm.hpp>
#include <ygm/detail/ygm_ptr.hpp>

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

  template <typename Function>
  void for_all(Function fn) {
    m_comm.barrier();
    local_for_all(fn);
  }

  void clear() {
    m_comm.barrier();
    m_local_bag.clear();
  }

  size_t size() {
    m_comm.barrier();
    return m_comm.all_reduce_sum(m_local_bag.size());
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
    std::for_each(m_local_bag.begin(), m_local_bag.end(), fn);
  }


  std::vector<value_type> gather_to_vector(int dest) {
    m_comm.barrier();
    std::vector<value_type> result;
    std::vector<std::vector<value_type>> buffers(m_comm.size());
    if(m_comm.rank() == dest) {
      ygm::ygm_ptr<std::vector<std::vector<value_type>>> res_ptr(&buffers);
      for(int i = 0; i < m_comm.size(); i++) {
        if(i == dest) {
          buffers[dest].insert(buffers[dest].begin(), m_local_bag.begin(), m_local_bag.end());
        } else {
          get_local_from_rank(res_ptr, i);
        }
      }
    }
    m_comm.barrier();
    if(m_comm.rank() == dest) {
      for(auto buffer : buffers) {
        for(auto elem : buffer) {
          result.push_back(elem);
        }
      }
    }
    return result;
  }

  void get_local_from_rank(ygm::ygm_ptr<std::vector<std::vector<value_type>>> res_ptr, int dest) {
    auto gatherer = [](auto mailbox, auto outer, auto res_ptr, int origin) {
      auto callback = [](auto mailbox, auto inner, auto res_ptr, int from, std::vector<value_type> data) {
        for(auto item : data)
          res_ptr->at(from).push_back(item);
      };
      outer->m_comm.async(origin, callback, outer->pthis, res_ptr, outer->m_comm.rank(), outer->m_local_bag);
    };
    m_comm.async(dest, gatherer, pthis, res_ptr, m_comm.rank());
  }

 protected:
  size_t                           m_round_robin = 0;
  ygm::comm                        m_comm;
  std::vector<value_type>          m_local_bag;
  typename ygm::ygm_ptr<self_type> pthis;
};
}  // namespace ygm::container::detail
