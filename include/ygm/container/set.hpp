// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once

#include <ygm/container/container_traits.hpp>
#include <ygm/container/container_traits.hpp>
#include <ygm/container/detail/base_async_insert.hpp>
#include <ygm/container/detail/base_async_erase.hpp>
#include <ygm/container/detail/base_async_contains.hpp>
#include <ygm/container/detail/base_async_insert_contains.hpp>
#include <ygm/container/detail/base_iteration.hpp>
#include <ygm/container/detail/base_count.hpp>
#include <ygm/container/detail/base_misc.hpp>
#include <ygm/container/detail/hash_partitioner.hpp>
#include <set>
// #include <ygm/container/detail/set_impl.hpp>

namespace ygm::container {

template <typename Value>
class set : public detail::base_async_insert<set<Value>, std::tuple<Value>>,
            public detail::base_async_erase<set<Value>, std::tuple<Value>>,
            public detail::base_async_contains<set<Value>, std::tuple<Value>>,
            public detail::base_async_insert_contains<set<Value>, std::tuple<Value>>,
            public detail::base_count<set<Value>, std::tuple<Value>>,
            public detail::base_misc<set<Value>, std::tuple<Value>>,
            public detail::base_iteration<set<Value>, std::tuple<Value>> { 
  friend class detail::base_misc<set<Value>, std::tuple<Value>>;

  public:

    using self_type         = set<Value>;
    using value_type        = Value;
    using size_type         = size_t;
    using for_all_args      = std::tuple<Value>;
    using container_type    = ygm::container::set_tag;

    set(ygm::comm& comm) : m_comm(comm), pthis(this), partitioner(comm, std::hash<value_type>()) {
      pthis.check(m_comm);
    }

    set(const self_type &other) 
        : m_comm(other.comm()), pthis(this), partitioner(other.comm, other.partitioner) {
      pthis.check(m_comm); // What is other.comm? Is this supposed to be other.comm()?
    }

    set(self_type &&other) noexcept
        : m_comm(other.comm()),
          pthis(this),
          partitioner(other.partitioner),
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


    void local_insert(const value_type &val) { m_local_set.insert(val); }

    void local_erase(const value_type &val) { m_local_set.erase(val); }

    void local_clear() { m_local_set.clear(); }

    size_t local_count(const value_type &val) const { return m_local_set.count(val); }

    size_t local_size() const { return m_local_set.size(); }

    template <typename Function>
    void local_for_all(Function fn) {
      std::for_each(m_local_set.begin(), m_local_set.end(), fn);
    }

    template <typename Function>
    void local_for_all(Function fn) const {
      std::for_each(m_local_set.cbegin(), m_local_set.cend(), fn);
    }

    // size_t count(const value_type &val) {
    //   m_comm.barrier();
    //   return m_comm.all_reduce_sum(m_local_set.count(val));
    // }

    void serialize(const std::string &fname) {
    }

    void deserialize(const std::string &fname) {
    }

    detail::hash_partitioner<std::hash<value_type>>  partitioner;

  private:

    void local_swap(self_type &other) {
      m_local_set.swap(other.m_local_set);
    }

    ygm::comm                         &m_comm;
    std::set<value_type>               m_local_set;
    typename ygm::ygm_ptr<self_type>   pthis; 
};

}  // namespace ygm::container
