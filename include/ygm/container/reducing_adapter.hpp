// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once
#include <ygm/container/detail/container_traits.hpp>
#include <ygm/detail/ygm_ptr.hpp>
#include <ygm/detail/ygm_traits.hpp>

namespace ygm::container {

template <typename Container, typename ReductionOp>
class reducing_adapter {
 public:
  using self_type         = reducing_adapter<Container, ReductionOp>;
  using key_type          = typename Container::key_type;
  using value_type        = typename Container::value_type;
  const size_t cache_size = 1024 * 1024;

  reducing_adapter(Container &c, ReductionOp reducer)
      : m_container(c), m_reducer(reducer), pthis(this) {
    m_cache.resize(cache_size);
  }

  void async_reduce(const key_type &key, const value_type &value) {
    cache_reduce(key, value);
  }

 private:
  struct cache_entry {
    key_type   key;
    value_type value;
    bool       occupied = false;
  };

  void cache_reduce(const key_type &key, const value_type &value) {
    if (m_cache_empty) {
      m_cache_empty = false;
      m_container.comm().register_pre_barrier_callback(
          [this]() { this->cache_flush_all(); });
    }

    // Bypass cache if current rank owns key
    if (m_container.comm().rank() == m_container.owner(key)) {
      container_reduction(key, value);
    } else {
      size_t slot = std::hash<key_type>{}(key) % cache_size;

      if (m_cache[slot].occupied == false) {
        m_cache[slot].key      = key;
        m_cache[slot].value    = value;
        m_cache[slot].occupied = true;
      } else {  // Slot is occupied
        if (m_cache[slot].key == key) {
          m_cache[slot].value = m_reducer(m_cache[slot].value, value);
        } else {
          cache_flush(slot);
          m_cache[slot].key      = key;
          m_cache[slot].value    = value;
          m_cache[slot].occupied = true;
        }
      }
    }
  }

  void cache_flush(const size_t slot) {
    container_reduction(m_cache[slot].key, m_cache[slot].value);
    m_cache[slot].occupied = false;
  }

  void cache_flush_all() {
    for (size_t i = 0; i < cache_size; ++i) {
      if (m_cache[i].occupied) {
        cache_flush(i);
      }
    }
    m_cache_empty = true;
  }

  void container_reduction(const key_type &key, const value_type &value) {
    if constexpr (ygm::container::detail::is_map<Container>) {
      m_container.async_reduce(key, value, m_reducer);
    } else if constexpr (ygm::container::detail::is_array<Container>) {
      m_container.async_binary_op_update_value(key, value, m_reducer);
    } else {
      static_assert(ygm::detail::always_false<>,
                    "Container unsuitable for reducing_adapter");
    }
  }

  std::vector<cache_entry> m_cache;
  bool                     m_cache_empty = true;

  Container                       &m_container;
  ReductionOp                      m_reducer;
  typename ygm::ygm_ptr<self_type> pthis;
};

template <typename Container, typename ReductionOp>
reducing_adapter<Container, ReductionOp> make_reducing_adapter(
    Container &c, ReductionOp reducer) {
  return reducing_adapter<Container, ReductionOp>(c, reducer);
}

}  // namespace ygm::container
