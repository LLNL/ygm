// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once

namespace ygm::container {

template <typename Value, typename Index>
array<Value, Index>::array(ygm::comm &comm, const size_type size)
    : m_global_size(size), m_default_value{}, m_comm(comm), pthis(this) {
  pthis.check(m_comm);

  resize(size);
}

template <typename Value, typename Index>
array<Value, Index>::array(ygm::comm &comm, const size_type size,
                           const mapped_type &dv)
    : m_default_value(dv), m_comm(comm), pthis(this) {
  pthis.check(m_comm);

  resize(size);
}

template <typename Value, typename Index>
array<Value, Index>::array(const self_type &rhs)
    : m_default_value(rhs.m_default_value),
      m_comm(rhs.m_comm),
      m_global_size(rhs.m_global_size),
      m_small_block_size(rhs.m_small_block_size),
      m_large_block_size(rhs.m_large_block_size),
      m_local_start_index(rhs.m_local_start_index),
      m_local_vec(rhs.m_local_vec),
      pthis(this) {
  pthis.check(m_comm);
}

template <typename Value, typename Index>
array<Value, Index>::~array() {
  m_comm.barrier();
}

template <typename Value, typename Index>
void array<Value, Index>::resize(const size_type    size,
                                 const mapped_type &fill_value) {
  m_comm.barrier();

  m_global_size      = size;
  m_small_block_size = size / m_comm.size();
  m_large_block_size = m_small_block_size + ((size / m_comm.size()) > 0);

  m_local_vec.resize(
      m_small_block_size + (m_comm.rank() < (size % m_comm.size())),
      fill_value);

  if (m_comm.rank() < (size % m_comm.size())) {
    m_local_start_index = m_comm.rank() * m_large_block_size;
  } else {
    m_local_start_index =
        (size % m_comm.size()) * m_large_block_size +
        (m_comm.rank() - (size % m_comm.size())) * m_small_block_size;
  }

  m_comm.barrier();
}

template <typename Value, typename Index>
void array<Value, Index>::resize(const size_type size) {
  resize(size, m_default_value);
}

template <typename Value, typename Index>
void array<Value, Index>::async_set(const key_type     index,
                                    const mapped_type &value) {
  ASSERT_RELEASE(index < m_global_size);
  auto putter = [](auto parray, const key_type i, const mapped_type &v) {
    key_type l_index = parray->local_index(i);
    ASSERT_RELEASE(l_index < parray->m_local_vec.size());
    parray->m_local_vec[l_index] = v;
  };

  int dest = owner(index);
  m_comm.async(dest, putter, pthis, index, value);
}

template <typename Value, typename Index>
template <typename BinaryOp>
void array<Value, Index>::async_binary_op_update_value(const key_type     index,
                                                       const mapped_type &value,
                                                       const BinaryOp    &b) {
  ASSERT_RELEASE(index < m_global_size);
  auto updater = [](const key_type i, mapped_type &v,
                    const mapped_type &new_value) {
    BinaryOp *binary_op;
    v = (*binary_op)(v, new_value);
  };

  async_visit(index, updater, value);
}
template <typename Value, typename Index>
template <typename UnaryOp>
void array<Value, Index>::async_unary_op_update_value(const key_type index,
                                                      const UnaryOp &u) {
  ASSERT_RELEASE(index < m_global_size);
  auto updater = [](const key_type i, mapped_type &v) {
    UnaryOp *u;
    v = (*u)(v);
  };

  async_visit(index, updater);
}

template <typename Value, typename Index>
template <typename Visitor, typename... VisitorArgs>
void array<Value, Index>::async_visit(const key_type index, Visitor visitor,
                                      const VisitorArgs &...args) {
  ASSERT_RELEASE(index < m_global_size);
  int  dest          = owner(index);
  auto visit_wrapper = [](auto parray, const key_type i,
                          const VisitorArgs &...args) {
    key_type l_index = parray->local_index(i);
    ASSERT_RELEASE(l_index < parray->m_local_vec.size());
    mapped_type &l_value = parray->m_local_vec[l_index];
    Visitor     *vis     = nullptr;
    if constexpr (std::is_invocable<decltype(visitor), const key_type &,
                                    mapped_type &, VisitorArgs &...>() ||
                  std::is_invocable<decltype(visitor), ptr_type,
                                    const key_type &, mapped_type &,
                                    VisitorArgs &...>()) {
      ygm::meta::apply_optional(*vis, std::make_tuple(parray),
                                std::forward_as_tuple(i, l_value, args...));
    } else {
      static_assert(
          ygm::detail::always_false<>,
          "remote array lambda signature must be invocable with (const "
          "&key_type, mapped_type&, ...) or (ptr_type, const "
          "&key_type, mapped_type&, ...) signatures");
    }
  };

  m_comm.async(dest, visit_wrapper, pthis, index,
               std::forward<const VisitorArgs>(args)...);
}

template <typename Value, typename Index>
template <typename Function>
void array<Value, Index>::for_all(Function fn) {
  m_comm.barrier();
  local_for_all(fn);
}

template <typename Value, typename Index>
template <typename Function>
void array<Value, Index>::local_for_all(Function fn) {
  if constexpr (std::is_invocable<decltype(fn), const key_type,
                                  mapped_type &>()) {
    for (int i = 0; i < m_local_vec.size(); ++i) {
      key_type g_index = global_index(i);
      fn(g_index, m_local_vec[i]);
    }
  } else if constexpr (std::is_invocable<decltype(fn), mapped_type &>()) {
    std::for_each(std::begin(m_local_vec), std::end(m_local_vec), fn);
  } else {
    static_assert(ygm::detail::always_false<>,
                  "local array lambda must be invocable with (const "
                  "key_type, mapped_type &) or (mapped_type &) signatures");
  }
}

template <typename Value, typename Index>
typename array<Value, Index>::size_type array<Value, Index>::size() {
  return m_global_size;
}

template <typename Value, typename Index>
typename array<Value, Index>::ptr_type array<Value, Index>::get_ygm_ptr()
    const {
  return pthis;
}

template <typename Value, typename Index>
ygm::comm &array<Value, Index>::comm() {
  return m_comm;
}

template <typename Value, typename Index>
const typename array<Value, Index>::mapped_type &
array<Value, Index>::default_value() const {
  return m_default_value;
}

template <typename Value, typename Index>
int array<Value, Index>::owner(const key_type index) const {
  int to_return;
  // Owner depends on whether index is before switching to small blocks
  if (index < (m_global_size % m_comm.size()) * m_large_block_size) {
    to_return = index / m_large_block_size;
  } else {
    to_return = (m_global_size % m_comm.size()) +
                (index - (m_global_size % m_comm.size()) * m_large_block_size) /
                    m_small_block_size;
  }
  ASSERT_RELEASE((to_return >= 0) && (to_return < m_comm.size()));

  return to_return;
}

template <typename Value, typename Index>
bool array<Value, Index>::is_mine(const key_type index) const {
  return owner(index) == m_comm.rank();
}

template <typename Value, typename Index>
typename array<Value, Index>::key_type array<Value, Index>::local_index(
    const key_type index) {
  key_type to_return = index - m_local_start_index;
  ASSERT_RELEASE((to_return >= 0) && (to_return <= m_small_block_size));
  return to_return;
}

template <typename Value, typename Index>
typename array<Value, Index>::key_type array<Value, Index>::global_index(
    const key_type index) {
  key_type to_return;
  return m_local_start_index + index;
}

};  // namespace ygm::container
