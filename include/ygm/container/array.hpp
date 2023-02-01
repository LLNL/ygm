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

  void async_set(const index_type index, const value_type& value) {
    m_impl.async_set(index, value);
  }

  template <typename BinaryOp>
  void async_binary_op_update_value(const index_type  index,
                                    const value_type& value,
                                    const BinaryOp&   b) {
    m_impl.async_binary_op_update_value(index, value, b);
  }

  void async_bit_and(const index_type index, const value_type& value) {
    async_binary_op_update_value(index, value, std::bit_and<value_type>());
  }

  void async_bit_or(const index_type index, const value_type& value) {
    async_binary_op_update_value(index, value, std::bit_or<value_type>());
  }

  void async_bit_xor(const index_type index, const value_type& value) {
    async_binary_op_update_value(index, value, std::bit_xor<value_type>());
  }

  void async_logical_and(const index_type index, const value_type& value) {
    async_binary_op_update_value(index, value, std::logical_and<value_type>());
  }

  void async_logical_or(const index_type index, const value_type& value) {
    async_binary_op_update_value(index, value, std::logical_or<value_type>());
  }

  void async_multiplies(const index_type index, const value_type& value) {
    async_binary_op_update_value(index, value, std::multiplies<value_type>());
  }

  void async_divides(const index_type index, const value_type& value) {
    async_binary_op_update_value(index, value, std::divides<value_type>());
  }

  void async_plus(const index_type index, const value_type& value) {
    async_binary_op_update_value(index, value, std::plus<value_type>());
  }

  void async_minus(const index_type index, const value_type& value) {
    async_binary_op_update_value(index, value, std::minus<value_type>());
  }

  template <typename UnaryOp>
  void async_unary_op_update_value(const index_type index, const UnaryOp& u) {
    m_impl.async_unary_op_update_value(index, u);
  }

  void async_increment(const index_type index) {
    async_unary_op_update_value(index,
                                [](const value_type& v) { return v + 1; });
  }

  void async_decrement(const index_type index) {
    async_unary_op_update_value(index,
                                [](const value_type& v) { return v - 1; });
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

  template <typename IntType, typename Function>
  void for_some(IntType count, Function fn) {
    m_impl.for_some(count, fn);
  }

  index_type size() { return m_impl.size(); }

  typename ygm::ygm_ptr<impl_type> get_ygm_ptr() const {
    return m_impl.get_ygm_ptr();
  }

  ygm::comm& comm() { return m_impl.comm(); }

 private:
  impl_type m_impl;
};

}  // namespace ygm::container
