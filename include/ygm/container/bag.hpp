// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once
#include <ygm/container/detail/bag_impl.hpp>

namespace ygm::container {
template <typename Item,
          typename SendBufferManager = ygm::locking_send_buffer_manager,
          typename Alloc = std::allocator<Item>, int NumBanks = 1024,
          typename LockBankTag = ygm::DefaultLockBankTag>
class bag {
public:
  using self_type = bag<Item, SendBufferManager, Alloc>;
  using value_type = Item;
  using impl_type =
      detail::bag_impl<Item, SendBufferManager, Alloc, NumBanks, LockBankTag>;

  bag(ygm::comm<SendBufferManager> &comm) : m_impl(comm) {}

  void async_insert(const value_type &item) { m_impl.async_insert(item); }

  template <typename Function> void for_all(Function fn) { m_impl.for_all(fn); }

  void clear() { m_impl.clear(); }

  size_t size() { return m_impl.size(); }

  void swap(self_type &s) { m_impl.swap(s.m_impl); }

  template <typename Function> void local_for_all(Function fn) {
    m_impl.local_for_all(fn);
  }

  void serialize(const std::string &fname) { m_impl.serialize(fname); }
  void deserialize(const std::string &fname) { m_impl.deserialize(fname); }

private:
  detail::bag_impl<value_type, SendBufferManager, Alloc, NumBanks, LockBankTag>
      m_impl;
};
} // namespace ygm::container
