// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once

#include <ygm/container/container_traits.hpp>
#include <ygm/random.hpp>

namespace ygm::container {
template <typename Item, typename Alloc = std::allocator<Item>>
class bag {
 public:
  using self_type          = bag<Item, Alloc>;
  using value_type         = Item;
  using size_type          = size_t;
  using ygm_for_all_types  = std::tuple<Item>;
  using ygm_container_type = ygm::container::bag_tag;

  bag(ygm::comm &comm);
  ~bag();

  void async_insert(const value_type &item);
  void async_insert(const value_type &item, int dest);
  void async_insert(const std::vector<value_type> &items, int dest);

  template <typename Function>
  void for_all(Function fn);

  void clear();

  size_type size();
  size_type local_size();

  void rebalance();

  void swap(self_type &s);

  template <typename RandomFunc>
  void local_shuffle(RandomFunc &r);
  void local_shuffle();

  template <typename RandomFunc>
  void global_shuffle(RandomFunc &r);
  void global_shuffle();

  template <typename Function>
  void local_for_all(Function fn);

  ygm::comm &comm();

  void                    serialize(const std::string &fname);
  void                    deserialize(const std::string &fname);
  std::vector<value_type> gather_to_vector(int dest);
  std::vector<value_type> gather_to_vector();

 private:
  std::vector<value_type> local_pop(int n);

  template <typename Function>
  void local_for_all_pair_types(Function fn);

 private:
  size_t                           m_round_robin = 0;
  ygm::comm                       &m_comm;
  std::vector<value_type>          m_local_bag;
  typename ygm::ygm_ptr<self_type> pthis;
};
}  // namespace ygm::container

#include <ygm/container/detail/bag.ipp>
