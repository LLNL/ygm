// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once
#include <vector>
#include <ygm/comm.hpp>
#include <ygm/detail/random.hpp>
#include <ygm/detail/ygm_ptr.hpp>

namespace ygm::container::detail {

template <typename Value, typename Index>
class array_impl {
 public:
  using self_type  = array_impl<Value, Index>;
  using value_type = Value;
  using index_type = Index;

  array_impl(ygm::comm &comm, const index_type size)
      : m_global_size(size), m_default_value{}, m_comm(comm), pthis(this) {
    pthis.check(m_comm);

    resize(size);
  }

  array_impl(ygm::comm &comm, const index_type size, const value_type &dv)
      : m_default_value(dv), m_comm(comm), pthis(this) {
    pthis.check(m_comm);

    resize(size);
  }

  array_impl(const self_type &rhs)
      : m_default_value(rhs.m_default_value),
        m_comm(rhs.m_comm),
        m_global_size(rhs.m_global_size),
        m_local_vec(rhs.m_local_vec),
        pthis(this) {}

  ~array_impl() { m_comm.barrier(); }

  void resize(const index_type size, const value_type &fill_value) {
    m_comm.barrier();

    m_global_size = size;
    m_block_size  = size / m_comm.size() + (size % m_comm.size() > 0);

    if (m_comm.rank() != m_comm.size() - 1) {
      m_local_vec.resize(m_block_size, fill_value);
    } else {
      // Last rank may get less data
      index_type block_size = m_global_size % m_block_size;
      if (block_size == 0) {
        block_size = m_block_size;
      }
      m_local_vec.resize(block_size, fill_value);
    }

    m_comm.barrier();
  }

  void resize(const index_type size) { resize(size, m_default_value); }

  void async_set(const index_type index, const value_type &value) {
    ASSERT_RELEASE(index < m_global_size);
    auto putter = [](auto parray, const index_type i, const value_type &v) {
      index_type l_index = parray->local_index(i);
      ASSERT_RELEASE(l_index < parray->m_local_vec.size());
      parray->m_local_vec[l_index] = v;
    };

    int dest = owner(index);
    m_comm.async(dest, putter, pthis, index, value);
  }

  template <typename BinaryOp>
  void async_binary_op_update_value(const index_type  index,
                                    const value_type &value,
                                    const BinaryOp   &b) {
    ASSERT_RELEASE(index < m_global_size);
    auto updater = [](const index_type i, value_type &v,
                      const value_type &new_value) {
      BinaryOp *binary_op;
      v = (*binary_op)(v, new_value);
    };

    async_visit(index, updater, value);
  }

  template <typename UnaryOp>
  void async_unary_op_update_value(const index_type index, const UnaryOp &u) {
    ASSERT_RELEASE(index < m_global_size);
    auto updater = [](const index_type i, value_type &v) {
      UnaryOp *u;
      v = (*u)(v);
    };

    async_visit(index, updater);
  }

  template <typename Visitor, typename... VisitorArgs>
  void async_visit(const index_type index, Visitor visitor,
                   const VisitorArgs &...args) {
    ASSERT_RELEASE(index < m_global_size);
    int  dest          = owner(index);
    auto visit_wrapper = [](auto parray, const index_type i,
                            const VisitorArgs &...args) {
      index_type l_index = parray->local_index(i);
      ASSERT_RELEASE(l_index < parray->m_local_vec.size());
      value_type &l_value = parray->m_local_vec[l_index];
      Visitor    *vis     = nullptr;
      ygm::meta::apply_optional(*vis, std::make_tuple(parray),
                                std::forward_as_tuple(i, l_value, args...));
    };

    m_comm.async(dest, visit_wrapper, pthis, index,
                 std::forward<const VisitorArgs>(args)...);
  }

  template <typename Function>
  void for_all(Function fn) {
    m_comm.barrier();
    for (int i = 0; i < m_local_vec.size(); ++i) {
      index_type g_index = global_index(i);
      fn(g_index, m_local_vec[i]);
    }
  }

  template <typename IntType, typename Function, typename RNG = std::mt19937>
  void local_for_random_samples(IntType count, Function fn,
                                RNG gen = std::mt19937{
                                    std::random_device{}()}) {
    m_comm.barrier();
    ASSERT_RELEASE(count < m_local_vec.size());
    std::vector<std::size_t> samples =
        random_subset(0, m_local_vec.size(), count, gen);
    for (const std::size_t sample : samples) {
      index_type g_index = global_index(sample);
      fn(g_index, m_local_vec[sample]);
    }
  }

  index_type size() { return m_global_size; }

  typename ygm::ygm_ptr<self_type> get_ygm_ptr() const { return pthis; }

  ygm::comm &comm() { return m_comm; }

  int owner(const index_type index) { return index / m_block_size; }

  index_type local_index(const index_type index) {
    return index % m_block_size;
  }

  index_type global_index(const index_type index) {
    return m_comm.rank() * m_block_size + index;
  }

 protected:
  array_impl() = delete;

  index_type                       m_global_size;
  index_type                       m_block_size;
  value_type                       m_default_value;
  std::vector<value_type>          m_local_vec;
  ygm::comm                       &m_comm;
  typename ygm::ygm_ptr<self_type> pthis;
};
}  // namespace ygm::container::detail
