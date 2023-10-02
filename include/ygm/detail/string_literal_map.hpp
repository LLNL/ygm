// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#include <ygm/detail/string_enumerator.hpp>

namespace ygm {
namespace detail {

template <typename Value>
class string_literal_map {
 public:
  using mapped_type = Value;

  string_literal_map() {
    m_values.resize(m_enumerator.get_num_items());
    m_key_mask.resize(m_enumerator.get_num_items());
  }

  template <StringLiteral S>
  mapped_type &get_value() {
    m_key_mask[m_enumerator.get_string_index<S>()] = true;
    return m_values[m_enumerator.get_string_index<S>()];
  }

  mapped_type &get_value_from_index(const size_t index) {
    return m_values[index];
  }

  size_t capacity() { return m_values.size(); }

  size_t size() {
    size_t to_return{0};
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

  bool is_filled(size_t index) { return m_key_mask[index]; }

 private:
  std::vector<mapped_type> m_values;
  std::vector<bool>        m_key_mask;

 public:
  string_enumerator m_enumerator;
};
}  // namespace detail
}  // namespace ygm
