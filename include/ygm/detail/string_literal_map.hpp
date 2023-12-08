// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once

#include <ygm/detail/string_enumerator.hpp>

namespace ygm {
namespace detail {

template <typename Value>
class string_literal_map;

template <typename Value>
void string_literal_map_match_keys(string_literal_map<Value> &, ygm::comm &);

template <typename Value>
class string_literal_map {
 public:
  using index_type  = string_enumerator::index_type;
  using key_type    = std::string;
  using mapped_type = Value;

  class iterator {
   public:
    iterator(string_literal_map &map, const size_t index)
        : m_map(map), m_index(index) {
      if (m_map.m_key_mask[m_index] == false) {
        this->operator++(1);
      }
    }

    bool operator==(const iterator &x) {
      return (m_index == x.m_index) && (&m_map == &x.m_map);
    }

    bool operator!=(const iterator &x) { return !(this->operator==(x)); }

    iterator operator++() { return this->operator++(1); }

    iterator operator++(int n) {
      // Increment m_index only on filled entries
      while (n > 0 && m_index < m_map.capacity()) {
        ++m_index;
        if (m_map.is_filled(m_index)) {
          --n;
        }
      }
      return *this;
    }

    std::pair<const key_type &, mapped_type &> operator*() {
      ASSERT_RELEASE(m_map.m_key_mask[m_index] == true);
      return std::pair<const key_type &, mapped_type &>(
          m_map.m_enumerator.get_string_by_index(m_index),
          m_map.m_values[m_index]);
    }

   private:
    string_literal_map &m_map;
    size_t              m_index;
  };

  string_literal_map() {
    m_values.resize(m_enumerator.get_num_items());
    m_key_mask.resize(m_enumerator.get_num_items());
  }

  iterator begin() { return iterator(*this, 0); }
  iterator end() { return iterator(*this, capacity()); }

  template <StringLiteral S>
  mapped_type &get_value() {
    m_key_mask[m_enumerator.get_string_index<S>()] = true;
    return m_values[m_enumerator.get_string_index<S>()];
  }

  const key_type &get_key_from_index(const index_type index) {
    return m_enumerator.get_string(index);
  }

  mapped_type &get_value_from_index(const index_type index) {
    m_key_mask[index] = true;
    return m_values[index];
  }

  index_type capacity() { return m_values.size(); }

  index_type size() {
    index_type to_return{0};
    for (const auto &b : m_key_mask) {
      if (b) {
        ++to_return;
      }
    }
    return to_return;
  }

  template <StringLiteral S>
  bool is_filled() {
    return m_key_mask[m_enumerator.get_string_index<S>()];
  }

  bool is_filled(index_type index) { return m_key_mask[index]; }

  friend void string_literal_map_match_keys<Value>(
      string_literal_map<Value> &str_map, ygm::comm &comm);

 private:
  std::vector<mapped_type> m_values;
  std::vector<bool>        m_key_mask;

  string_enumerator m_enumerator;
};
}  // namespace detail
}  // namespace ygm
