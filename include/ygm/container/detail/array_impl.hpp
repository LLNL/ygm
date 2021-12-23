// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once
#include <vector>
#include <ygm/comm.hpp>
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

    index_type curr_local_size = m_local_vec.size();

    index_type local_size =
        (size / m_comm.size()) + (m_comm.rank() < (size % m_comm.size()));
    m_local_vec.resize(local_size, fill_value);

    m_comm.barrier();
  }

  void resize(const index_type size) { resize(size, m_default_value); }

  void async_put(const index_type index, const value_type &value) {
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
                                    const BinaryOp &  b) {
    ASSERT_RELEASE(index < m_global_size);
    auto updater = [](const index_type i, value_type &v,
                      const value_type &new_value) {
      v = BinaryOp()(v, new_value);
    };

    async_visit(index, updater, value);
  }

  template <typename Visitor, typename... VisitorArgs>
  void async_visit(const index_type index, Visitor visitor,
                   const VisitorArgs &... args) {
    ASSERT_RELEASE(index < m_global_size);
    int  dest          = owner(index);
    auto visit_wrapper = [](auto parray, const index_type i,
                            const VisitorArgs &... args) {
      index_type l_index = parray->local_index(i);
      ASSERT_RELEASE(l_index < parray->m_local_vec.size());
      value_type &l_value = parray->m_local_vec[l_index];
      Visitor *   vis     = nullptr;
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

  ygm::comm &comm() { return m_comm; }

  int owner(const index_type index) { return index % m_comm.size(); }

  index_type local_index(const index_type index) {
    return index / m_comm.size();
  }

  index_type global_index(const index_type index) {
    return m_comm.size() * index + m_comm.rank();
  }

 protected:
  array_impl() = delete;

  index_type                       m_global_size;
  value_type                       m_default_value;
  std::vector<value_type>          m_local_vec;
  ygm::comm &                      m_comm;
  typename ygm::ygm_ptr<self_type> pthis;
};
}  // namespace ygm::container::detail
