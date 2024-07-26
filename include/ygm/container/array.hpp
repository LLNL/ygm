// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once

#include <ygm/comm.hpp>
#include <ygm/container/container_traits.hpp>
#include <ygm/container/detail/base_async_insert.hpp>
#include <ygm/container/detail/base_async_visit.hpp>
#include <ygm/container/detail/base_iteration.hpp>
#include <ygm/container/detail/base_misc.hpp>
#include <ygm/container/detail/block_partitioner.hpp>

namespace ygm::container {

template <typename Value, typename Index = size_t>
class array
    : public detail::base_async_insert_key_value<array<Value, Index>,
                                                 std::tuple<Index, Value>>,
      public detail::base_misc<array<Value, Index>, std::tuple<Index, Value>>,
      public detail::base_async_visit<array<Value, Index>,
                                      std::tuple<Index, Value>>,
      public detail::base_iteration<array<Value, Index>,
                                    std::tuple<Index, Value>> {
  friend class detail::base_misc<array<Value, Index>, std::tuple<Index, Value>>;

 public:
  using self_type      = array<Value, Index>;
  using mapped_type    = Value;
  using key_type       = Index;
  using size_type      = Index;
  using for_all_args   = std::tuple<Index, Value>;
  using container_type = ygm::container::array_tag;
  using ptr_type       = typename ygm::ygm_ptr<self_type>;

  // Pull in async_visit for use within the array
  using detail::base_async_visit<array<Value, Index>,
                                 std::tuple<Index, Value>>::async_visit;

  array() = delete;

  array(ygm::comm& comm, const size_type size)
      : m_global_size(size),
        m_default_value{},
        m_comm(comm),
        pthis(this),
        partitioner(comm, size) {
    pthis.check(m_comm);

    resize(size);
  }

  array(ygm::comm& comm, const size_type size, const mapped_type& default_value)
      : m_global_size(size),
        m_default_value(default_value),
        m_comm(comm),
        pthis(this),
        partitioner(comm, size) {
    pthis.check(m_comm);

    resize(size);
  }

  array(const self_type& rhs)
      : m_global_size(rhs.m_global_size),
        m_default_value(rhs.m_default_value),
        m_local_vec(rhs.m_local_vec),
        m_comm(rhs.m_comm),
        partitioner(m_comm, m_global_size) {
    pthis.check(m_comm);
    resize(m_global_size);
  }

  ~array() { m_comm.barrier(); }

  void local_insert(const key_type& key, const mapped_type& value) {
    m_local_vec[partitioner.local_index(key)] = value;
  }

  template <typename Function, typename... VisitorArgs>
  void local_visit(const key_type index, Function& fn,
                   const VisitorArgs&... args) {
    ygm::detail::interrupt_mask mask(m_comm);
    if constexpr (std::is_invocable<decltype(fn), const key_type, mapped_type&,
                                    VisitorArgs&...>() ||
                  std::is_invocable<decltype(fn), ptr_type, const key_type,
                                    mapped_type&, VisitorArgs&...>()) {
      ygm::meta::apply_optional(
          fn, std::make_tuple(pthis),
          std::forward_as_tuple(
              index, m_local_vec[partitioner.local_index(index)], args...));
    } else {
      static_assert(ygm::detail::always_false<>,
                    "remote array lambda must be "
                    "invocable with (const "
                    "key_type, mapped_type &, ...) or "
                    "(ptr_type, mapped_type &, ...) signatures");
    }
  }

  void async_set(const key_type index, const mapped_type& value) {
    detail::base_async_insert_key_value<array<Value, Index>,
                                        for_all_args>::async_insert(index,
                                                                    value);
  }

  template <typename BinaryOp>
  void async_binary_op_update_value(const key_type     index,
                                    const mapped_type& value,
                                    const BinaryOp&    b) {
    ASSERT_RELEASE(index < m_global_size);
    auto updater = [](const key_type i, mapped_type& v,
                      const mapped_type& new_value) {
      BinaryOp* binary_op;
      v = (*binary_op)(v, new_value);
    };

    async_visit(index, updater, value);
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
    ASSERT_RELEASE(index < m_global_size);
    auto updater = [](const key_type i, mapped_type& v) {
      UnaryOp* u;
      v = (*u)(v);
    };

    async_visit(index, updater);
  }

  void async_increment(const key_type index) {
    async_unary_op_update_value(index,
                                [](const mapped_type& v) { return v + 1; });
  }

  void async_decrement(const key_type index) {
    async_unary_op_update_value(index,
                                [](const mapped_type& v) { return v - 1; });
  }

  const mapped_type& default_value() const;

  void resize(const size_type size, const mapped_type& fill_value) {
    m_comm.barrier();

    // Copy current values into temporary vector for storing in
    // ygm::container::array after resizing local array structures
    std::vector<std::pair<const key_type, const mapped_type>> tmp_values;
    tmp_values.reserve(local_size());
    local_for_all(
        [&tmp_values](const key_type& index, const mapped_type& value) {
          tmp_values.push_back(std::make_pair(index, value));
        });

    m_global_size = size;
    partitioner   = detail::block_partitioner<key_type>(m_comm, size);

    m_local_vec.resize(partitioner.local_size(), fill_value);

    m_default_value = fill_value;

    // Repopulate array values
    for (const auto& [index, value] : tmp_values) {
      if (index < size) {
        async_set(index, value);
      }
    }

    m_comm.barrier();
  }

  size_t local_size() { return partitioner.local_size(); }

  size_t size() const {
    m_comm.barrier();
    return m_global_size;
  }

  void resize(const size_type size) { resize(size, m_default_value); }

  void local_clear() { resize(0); }

  void local_swap(self_type& other) {
    m_local_vec.swap(other.m_local_vec);
    std::swap(m_global_size, other.m_global_size);
    std::swap(m_default_value, other.m_default_value);
    std::swap(partitioner, other.partitioner);
  }

  template <typename Function>
  void local_for_all(Function fn) {
    if constexpr (std::is_invocable<decltype(fn), const key_type,
                                    mapped_type&>()) {
      for (int i = 0; i < m_local_vec.size(); ++i) {
        key_type g_index = partitioner.global_index(i);
        fn(g_index, m_local_vec[i]);
      }
    } else if constexpr (std::is_invocable<decltype(fn), mapped_type&>()) {
      std::for_each(std::begin(m_local_vec), std::end(m_local_vec), fn);
    } else {
      static_assert(ygm::detail::always_false<>,
                    "local array lambda must be "
                    "invocable with (const "
                    "key_type, mapped_type &) or "
                    "(mapped_type &) signatures");
    }
  }

  detail::block_partitioner<key_type> partitioner;

 private:
  size_type                        m_global_size;
  mapped_type                      m_default_value;
  std::vector<mapped_type>         m_local_vec;
  ygm::comm&                       m_comm;
  typename ygm::ygm_ptr<self_type> pthis;
};

}  // namespace ygm::container
