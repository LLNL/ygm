// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once
#include <ygm/comm.hpp>
#include <ygm/container/map.hpp>
#include <ygm/detail/ygm_ptr.hpp>

namespace ygm::container {

template <typename Item, typename Alloc = std::allocator<Item>>
class tagged_bag {
 public:
  using tag_type   = size_t;
  using value_type = Item;
  using self_type  = tagged_bag<Item, Alloc>;

  tagged_bag(const tagged_bag &)                = delete;
  tagged_bag(tagged_bag &&) noexcept            = delete;
  tagged_bag &operator=(const tagged_bag &)     = delete;
  tagged_bag &operator=(tagged_bag &&) noexcept = delete;
  tagged_bag(ygm::comm &comm)
      : m_next_tag(tag_type(comm.rank()) << TAG_BITS),
        m_tagged_bag(ygm::container::map<tag_type, value_type>(comm)),
        pthis(this) {}

  tag_type async_insert(const value_type &item) {
    tag_type tag = m_next_tag++;
    m_tagged_bag.async_insert(tag, item);
    return tag;
  }

  template <typename Visitor, typename... VisitorArgs>
  void async_visit(const tag_type &tag, Visitor visitor,
                   const VisitorArgs &...args) {
    return m_tagged_bag.async_visit(tag, visitor, args...);
  }

  template <typename Visitor, typename... VisitorArgs>
  void async_visit_if_exists(const tag_type &tag, Visitor visitor,
                             const VisitorArgs &...args) {
    return m_tagged_bag.async_visit_if_exists(tag, visitor, args...);
  }

  void async_erase(const tag_type &tag) {
    return m_tagged_bag.async_erase(tag);
  }

  template <typename Function>
  void for_all(Function fn) {
    return m_tagged_bag.for_all(fn);
  }

  void clear() { return m_tagged_bag.clear(); }

  size_t size() { return m_tagged_bag.size(); }

  void swap(self_type &s) {
    m_tagged_bag.swap(s.m_tagged_bag);
    std::swap(m_next_tag, s.m_next_tag);
  }

  ygm::comm &comm() { return m_tagged_bag.comm(); }

  // TODO sbromberger 20230626: serialize and deserialize

  [[nodiscard]] int owner(const tag_type &tag) const {
    return m_tagged_bag.owner(tag);
  }

  [[nodiscard]] bool is_mine(const tag_type &tag) const {
    return m_tagged_bag.is_mine(tag);
  }

  std::vector<value_type> local_get(const tag_type &tag) {
    return m_tagged_bag.local_get(tag);
  }

  template <typename Function, typename... VisitorArgs>
  void local_visit(const tag_type &tag, Function &fn,
                   const VisitorArgs &...args) {
    return m_tagged_bag.local_visit(tag, fn, args...);
  }

  void local_erase(const tag_type &tag) { m_tagged_bag.m_local_map.erase(tag); }

  void local_clear() { m_tagged_bag.m_local_map.clear(); }

  [[nodiscard]] size_t local_size() const {
    return m_tagged_bag.m_local_map.size();
  }

  template <typename STLKeyContainer>
  std::map<tag_type, value_type> all_gather(const STLKeyContainer &tags) {
    return m_tagged_bag.all_gather(tags);
  }

  std::map<tag_type, value_type> all_gather(const std::vector<tag_type> &tags) {
    return m_tagged_bag.all_gather(tags);
  }
  template <typename Function>
  void local_for_all(Function fn) {
    return m_tagged_bag.local_for_all(fn);
  }

 private:
  // round_robin max is 2^TAG_BITS
  const tag_type TAG_BITS = 40;
  const tag_type MAX_TAGS = (size_t(1) << TAG_BITS) - 1;
  tag_type       m_next_tag;
  ygm::container::map<tag_type, value_type> m_tagged_bag;
  typename ygm::ygm_ptr<self_type>          pthis;
};
}  // namespace ygm::container
