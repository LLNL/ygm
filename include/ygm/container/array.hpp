// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once

#include <ygm/container/detail/array_impl.hpp>
namespace ygm::container {

template <typename Value, typename Index = size_t>
class array {
 public:
  using self_type  = array<Value, Index>;
  using value_type = Value;
  using index_type = Index;
  using impl_type  = detail::array_impl<value_type, index_type>;

  array() = delete;

  array(ygm::comm& comm, const index_type size) : m_impl(comm, size) {}

  array(ygm::comm& comm, const index_type size, const value_type& default_value)
      : m_impl(comm, size, default_value) {}

  array(const self_type& rhs) : m_impl(rhs.m_impl) {}

  void resize(const index_type size) { m_impl.resize(size); }

  void resize(const index_type size, const value_type& value) {
    m_impl.resize(size, value);
  }

  void async_put(const index_type index, const value_type& value) {
    m_impl.async_put(index, value);
  }

  template <typename BinaryOp>
  void async_binary_op_update_value(const index_type  index,
                                    const value_type& value,
                                    const BinaryOp&   b) {
    m_impl.async_binary_op_update_value(index, value, b);
  }

  template <typename Visitor, typename... VisitorArgs>
  void async_visit(const index_type index, Visitor visitor,
                   const VisitorArgs&... args) {
    m_impl.async_visit(index, visitor,
                       std::forward<const VisitorArgs>(args)...);
  }

  template <typename Function>
  void for_all(Function fn) {
    m_impl.for_all(fn);
  }

  ygm::comm& comm() { return m_impl.comm(); }

 private:
  impl_type m_impl;
};

}  // namespace ygm::container
