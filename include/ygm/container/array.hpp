// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once

#include <ygm/comm.hpp>
#include <ygm/container/container_traits.hpp>

namespace ygm::container {

template <typename Value, typename Index = size_t>
class array {
 public:
  using self_type          = array<Value, Index>;
  using mapped_type        = Value;
  using key_type           = Index;
  using size_type          = Index;
  using ygm_for_all_types  = std::tuple<Index, Value>;
  using ygm_container_type = ygm::container::array_tag;
  using ptr_type           = typename ygm::ygm_ptr<self_type>;

  array() = delete;

  array(ygm::comm& comm, const size_type size);

  array(ygm::comm& comm, const size_type size,
        const mapped_type& default_value);

  array(const self_type& rhs);

  ~array();

  void async_set(const key_type index, const mapped_type& value);

  template <typename BinaryOp>
  void async_binary_op_update_value(const key_type     index,
                                    const mapped_type& value,
                                    const BinaryOp&    b);

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
  void async_unary_op_update_value(const key_type index, const UnaryOp& u);

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
                   const VisitorArgs&... args);

  template <typename Function>
  void for_all(Function fn);

  size_type size();

  typename ygm::ygm_ptr<self_type> get_ygm_ptr() const;

  int owner(const key_type index) const;

  bool is_mine(const key_type index) const;

  ygm::comm& comm();

  const mapped_type& default_value() const;

  void resize(const size_type size, const mapped_type& fill_value);

  void resize(const size_type size);

 private:
  template <typename Function>
  void local_for_all(Function fn);

  key_type local_index(key_type index);

  key_type global_index(key_type index);

 private:
  size_type                        m_global_size;
  size_type                        m_small_block_size;
  size_type                        m_large_block_size;
  size_type                        m_local_start_index;
  mapped_type                      m_default_value;
  std::vector<mapped_type>         m_local_vec;
  ygm::comm&                       m_comm;
  typename ygm::ygm_ptr<self_type> pthis;
};

}  // namespace ygm::container

#include <ygm/container/detail/array.ipp>
