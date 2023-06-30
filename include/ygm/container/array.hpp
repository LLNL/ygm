// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once

#include <ygm/container/detail/array_impl.hpp>
#include <ygm/container/container_traits.hpp>

namespace ygm::container {

template <typename Value, typename Index = size_t>
class array {
 public:
  using self_type           = array<Value, Index>;
  using mapped_type         = Value;
  using key_type            = Index;
  using size_type           = Index;
  using ygm_for_all_types   = std::tuple< Index, Value >;
  using ygm_container_type  = ygm::container::array_tag;
  using impl_type           = detail::array_impl<mapped_type, key_type>;

  array() = delete;

  array(ygm::comm& comm, const size_type size) : m_impl(comm, size) {}

  array(ygm::comm& comm, const size_type size, const mapped_type& default_value)
      : m_impl(comm, size, default_value) {}

  array(const self_type& rhs) : m_impl(rhs.m_impl) {}

  void async_set(const key_type index, const mapped_type& value) {
    m_impl.async_set(index, value);
  }

  template <typename BinaryOp>
  void async_binary_op_update_value(const key_type  index,
                                    const mapped_type& value,
                                    const BinaryOp&   b) {
    m_impl.async_binary_op_update_value(index, value, b);
  }

  void async_bit_and(const key_type index, const mapped_type& value) {
    async_binary_op_update_value(index, value, std::bit_and<mapped_type>());
  }

  void async_bit_or(const key_type index, const mapped_type& value) {
    async_binary_op_update_value(index, value, std::bit_or<mapped_type>());
  }

  void async_bit_xor(const key_type index, const mapped_type& value) {
    async_binary_op_update_value(index, value, std::bit_xor<mapped_type>());
  }

  void async_logical_and(const key_type index, const mapped_type& value) {
    async_binary_op_update_value(index, value, std::logical_and<mapped_type>());
  }

  void async_logical_or(const key_type index, const mapped_type& value) {
    async_binary_op_update_value(index, value, std::logical_or<mapped_type>());
  }

  void async_multiplies(const key_type index, const mapped_type& value) {
    async_binary_op_update_value(index, value, std::multiplies<mapped_type>());
  }

  void async_divides(const key_type index, const mapped_type& value) {
    async_binary_op_update_value(index, value, std::divides<mapped_type>());
  }

  void async_plus(const key_type index, const mapped_type& value) {
    async_binary_op_update_value(index, value, std::plus<mapped_type>());
  }

  void async_minus(const key_type index, const mapped_type& value) {
    async_binary_op_update_value(index, value, std::minus<mapped_type>());
  }

  template <typename UnaryOp>
  void async_unary_op_update_value(const key_type index, const UnaryOp& u) {
    m_impl.async_unary_op_update_value(index, u);
  }

  void async_increment(const key_type index) {
    async_unary_op_update_value(index,
                                [](const mapped_type& v) { return v + 1; });
  }

  void async_decrement(const key_type index) {
    async_unary_op_update_value(index,
                                [](const mapped_type& v) { return v - 1; });
  }

  template <typename Visitor, typename... VisitorArgs>
  void async_visit(const key_type index, Visitor visitor,
                   const VisitorArgs&... args) {
    m_impl.async_visit(index, visitor,
                       std::forward<const VisitorArgs>(args)...);
  }

  template <typename Function>
  void for_all(Function fn) {
    m_impl.for_all(fn);
  }

  size_type size() { return m_impl.size(); }

  typename ygm::ygm_ptr<impl_type> get_ygm_ptr() const {
    return m_impl.get_ygm_ptr();
  }

  int owner(const key_type index) const { return m_impl.owner(index); }

  bool is_mine(const key_type index) const { return m_impl.is_mine(index); }

  ygm::comm& comm() { return m_impl.comm(); }

  const mapped_type& default_value() const { return m_impl.default_value(); }

 private:
  impl_type m_impl;
};

}  // namespace ygm::container
