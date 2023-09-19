// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once

#include <vector>

#include <ygm/detail/hash.hpp>

#define ADD_ENUMERATOR_ITEM(type, x)                                          \
  get_item_index(                                                             \
      (x), ygm::detail::constexpr_enumerator<type>::dummy<ygm::detail::crc32( \
               (x))>())

namespace ygm {
namespace detail {

// Class that assigns contiguous IDs to all registered constexpr objects of type
// T. Objects must be hashable at compile time to assign unique types to all
// objects (currently works for string and integral types). At runtime, object
// is recorded the first time get_item_index() is called (through
// ADD_ENUMERATOR_ITEM macro). No checking of hash collisions is performed.
template <typename T>
class constexpr_enumerator {
 public:
  template <unsigned int EntryHash>
  class entry {
   public:
    const static int   id;
    static inline bool is_set = false;
  };

  template <unsigned int EntryHash>
  class dummy {};

  constexpr_enumerator();

  // Returns index of item in m_vec and sets entry in m_vec if not already set
  template <unsigned int EntryHash>
  static int get_item_index(const T &t, dummy<EntryHash> d) {
    if (not entry<EntryHash>::is_set) {
      record_item(t, d);
    }
    return entry<EntryHash>::id;
  }

  // Sets entry in m_vec
  template <unsigned int EntryHash>
  static void record_item(const T &t, dummy<EntryHash> d) {
    assert(entry<EntryHash>::id < m_vec.size());
    m_vec[entry<EntryHash>::id] = t;
    entry<EntryHash>::is_set    = true;
  }

  size_t get_num_items() { return m_counter; }

  const T &get_item(const size_t index) { return m_vec[index]; }

  template <unsigned int EntryHash>
  static int next_counter() {
    return m_counter++;
  }

 private:
  static int m_counter;

 public:
  static std::vector<T> m_vec;
};
template <typename T>
std::vector<T> constexpr_enumerator<T>::m_vec;
template <typename T>
int constexpr_enumerator<T>::m_counter = 0;

template <typename T>
template <unsigned int N>
const int constexpr_enumerator<T>::entry<N>::id =
    constexpr_enumerator::next_counter<N>();

// Use constexpr_enumerator constructor to resize vector of entries after
// next_counter() has been called for each entry
template <typename T>
constexpr_enumerator<T>::constexpr_enumerator() {
  constexpr_enumerator<T>::m_vec.resize(constexpr_enumerator<T>::m_counter);
}

}  // namespace detail
}  // namespace ygm
