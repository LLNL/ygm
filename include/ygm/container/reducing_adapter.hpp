// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once

namespace ygm::container {

template <typename Container, typename ReductionOp>
class reducing_adapter {
 public:
  reducing_adapter(Container &c, ReductionOp reducer) : m_container(c) {}

  void async_reduce(const typename Container::key_type   &key,
                    const typename Container::value_type &value) {
    ReductionOp *r;
    m_container.async_reduce(key, value, *r);
  }

 private:
  Container &m_container;
};

template <typename Container, typename ReductionOp>
reducing_adapter<Container, ReductionOp> make_reducing_adapter(
    Container &c, ReductionOp reducer) {
  return reducing_adapter<Container, ReductionOp>(c, reducer);
}

}  // namespace ygm::container
