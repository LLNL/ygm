// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once

#include <ygm/container/container_traits.hpp>
#include <ygm/container/detail/set_impl.hpp>

namespace ygm::container {
template <typename Item>
class set : public detail::base_async_insert<set<Item>, std::tuple<Item>>,
            public detail::base_misc<set<Item>, std::tuple<Item>>,
            public detail::base_iteration<set<Item>, std::tuple<Item>> { 
  friend class detail::base_misc<set<Item>, std::tuple<Item>;

  public:

    using self_type         = set<Item>;
    using value_type        = Item;
    using size_type         = size_t;
    using for_all_args      = std::tuple<Item>;
    using container_type    = ygm::container::set_tag;

    set(ygm::comm& comm) : m_comm(comm), partitionier(comm) {
      pthis.check(m_comm);
    }

    set(const self_type &other) 
        : m_comm(other.comm), pthis(this), partitioner(other.comm) {
      pthis.check(m_comm); // What is other.comm? Is this supposed to be other.comm()?
    }

    set(self_type &&other) noexcept
        : m_comm(other.comm),
          pthis(this),
          partitioner(other.comm),
          m_local_set(std::move(other.m_local_set)) {
      pthis.check(m_comm);
    }

    ~set() { m_comm.barrier(); }

    set() = delete;

    set &operator=(const self_type &other) {
      return *this = set(other);
    }

    set &operator=(self_type &&other) noexcept {
      std::swap(m_local_set, other.m_local_set);
      return *this;
    }

    using detail::base_async_insert<set<Item>, for_all_args>::async_insert;

    void local_insert(const Item &val) { m_local_set.insert(val); }

    void local_clear() { m_local_set.clear(); }

    size_t local_size() const { return m_local_set.size(); }

    template <typename Function>
    void local_for_all(Function fn) {
      std::for_each(m_local_set.begin(), m_local_set.end(), fn)
    }

    template <typename Function>
    void local_for_all(Function fn) const {
      std::for_each(m_local_set.cbegin(), m_local_set.cend(), fn)
    }

    void serialize(const std::string &fname) {
    }

    void deserialize(const std::string &fname) {
    }

    detail::hash_partitioner             partitioner;

  private:

    void local_swap(self_type &other) {
      m_local_set.swap(other.m_local_set);
    }

    ygm::comm                         &m_comm;
    std::set<value_type>               m_local_set;
    typename ygm::ygm_ptr<self_type>   pthis; 
};

}  // namespace ygm::container
