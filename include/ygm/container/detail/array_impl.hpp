// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once
#include <vector>
#include <ygm/comm.hpp>
#include <ygm/detail/ygm_ptr.hpp>
#include <ygm/detail/ygm_traits.hpp>
#include <ygm/container/container_traits.hpp>

namespace ygm::container::detail {

template <typename Value, typename Index>
class array_impl {
 public:
  using self_type           = array_impl<Value, Index>;
  using ptr_type            = typename ygm::ygm_ptr<self_type>;
  using mapped_type         = Value;
  using key_type            = Index;
  using size_type           = Index;
  using ygm_for_all_types   = std::tuple< Index, Value >;
  using ygm_container_type  = ygm::container::array_tag;

  array_impl(ygm::comm &comm, const size_type size)
      : m_global_size(size), m_default_value{}, m_comm(comm), pthis(this) {
    pthis.check(m_comm);

    resize(size);
  }

  array_impl(ygm::comm &comm, const size_type size, const mapped_type &dv)
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

  void resize(const size_type size, const mapped_type &fill_value) {
    m_comm.barrier();

    m_global_size = size;
    m_block_size  = size / m_comm.size() + (size % m_comm.size() > 0);

    if (m_comm.rank() != m_comm.size() - 1) {
      m_local_vec.resize(m_block_size, fill_value);
    } else {
      // Last rank may get less data
      size_type block_size = m_global_size % m_block_size;
      if (block_size == 0) {
        block_size = m_block_size;
      }
      m_local_vec.resize(block_size, fill_value);
    }

    m_comm.barrier();
  }

  void resize(const size_type size) { resize(size, m_default_value); }

  void async_set(const key_type index, const mapped_type &value) {
    ASSERT_RELEASE(index < m_global_size);
    auto putter = [](auto parray, const key_type i, const mapped_type &v) {
      key_type l_index = parray->local_index(i);
      ASSERT_RELEASE(l_index < parray->m_local_vec.size());
      parray->m_local_vec[l_index] = v;
    };

    int dest = owner(index);
    m_comm.async(dest, putter, pthis, index, value);
  }

  template <typename BinaryOp>
  void async_binary_op_update_value(const key_type  index,
                                    const mapped_type &value,
                                    const BinaryOp   &b) {
    ASSERT_RELEASE(index < m_global_size);
    auto updater = [](const key_type i, mapped_type &v,
                      const mapped_type &new_value) {
      BinaryOp *binary_op;
      v = (*binary_op)(v, new_value);
    };

    async_visit(index, updater, value);
  }

  template <typename UnaryOp>
  void async_unary_op_update_value(const key_type index, const UnaryOp &u) {
    ASSERT_RELEASE(index < m_global_size);
    auto updater = [](const key_type i, mapped_type &v) {
      UnaryOp *u;
      v = (*u)(v);
    };

    async_visit(index, updater);
  }

  template <typename Visitor, typename... VisitorArgs>
  void async_visit(const key_type index, Visitor visitor,
                   const VisitorArgs &...args) {
    ASSERT_RELEASE(index < m_global_size);
    int  dest          = owner(index);
    auto visit_wrapper = [](auto parray, const key_type i,
                            const VisitorArgs &...args) {
      key_type l_index = parray->local_index(i);
      ASSERT_RELEASE(l_index < parray->m_local_vec.size());
      mapped_type &l_value = parray->m_local_vec[l_index];
      Visitor    *vis     = nullptr;
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

  template <typename Function>
  void for_all(Function fn) {
    m_comm.barrier();
    local_for_all(fn);
  }

  template <typename Function>
  void local_for_all(Function fn) {
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

  size_type size() { return m_global_size; }

  typename ygm::ygm_ptr<self_type> get_ygm_ptr() const { return pthis; }

  ygm::comm &comm() { return m_comm; }

  const mapped_type &default_value() const { return m_default_value; }

  int owner(const key_type index) const { return index / m_block_size; }

  bool is_mine(const key_type index) const {
    return owner(index) == m_comm.rank();
  }

  key_type local_index(const key_type index) {
    return index % m_block_size;
  }

  key_type global_index(const key_type index) {
    return m_comm.rank() * m_block_size + index;
  }

 protected:
  array_impl() = delete;

  size_type                          m_global_size;
  size_type                          m_block_size;
  mapped_type                        m_default_value;
  std::vector<mapped_type>           m_local_vec;
  ygm::comm                          &m_comm;
  typename ygm::ygm_ptr<self_type>   pthis;
};
}  // namespace ygm::container::detail
