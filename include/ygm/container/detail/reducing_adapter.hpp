// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once
#include <ygm/container/detail/container_traits.hpp>
#include <ygm/detail/ygm_traits.hpp>

namespace ygm::container::detail {

template <typename Container, typename ReductionOp>
class reducing_adapter {
 public:
  reducing_adapter(Container &c, ReductionOp reducer)
      : m_container(c), m_reducer(reducer) {}

  void async_reduce(const typename Container::key_type   &key,
                    const typename Container::value_type &value) {
    if constexpr (ygm::container::detail::is_map<Container>) {
      m_container.async_reduce(key, value, m_reducer);
    } else if constexpr (ygm::container::detail::is_array<Container>) {
      m_container.async_binary_op_update_value(key, value, m_reducer);
    } else {
      static_assert(ygm::detail::always_false<>,
                    "Container unsuitable for reducing_adapter");
    }
  }

 private:
  Container  &m_container;
  ReductionOp m_reducer;
};

template <typename Container, typename ReductionOp>
reducing_adapter<Container, ReductionOp> make_reducing_adapter(
    Container &c, ReductionOp reducer) {
  return reducing_adapter<Container, ReductionOp>(c, reducer);
}

}  // namespace ygm::container::detail
