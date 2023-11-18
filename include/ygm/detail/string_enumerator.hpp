// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once

#include <algorithm>
#include <array>
#include <iostream>
#include <vector>

namespace ygm {
class comm;
namespace detail {
// Class to hold constexpr string.
// Constexpr constructor used to allow templates to appear to accept constexpr
// strings as template parameters (requires C++20 features)
template <size_t N>
struct StringLiteral {
  constexpr StringLiteral<N>(const char (&str)[N]) : value("") {
    for (size_t i = 0; i < N; ++i) {
      value[i] = str[i];
    }
  }

  char                    value[N];
  constexpr static size_t len = N;
};

class string_enumerator {
 public:
  using index_type = int;

  template <StringLiteral S>
  class entry;

  string_enumerator();

  bool operator==(const string_enumerator &x) const {
    if (get_num_items() != x.get_num_items()) {
      return false;
    }

    bool to_return = true;
    for (size_t i = 0; i < get_num_items(); ++i) {
      to_return &= (get_string_by_index(i) == x.get_string_by_index(i));
    }
    return to_return;
  }

  template <class Archive>
  void serialize(Archive &archive) {
    archive(m_counter);
    archive(m_vec);
  }

  template <StringLiteral S>
  static index_type get_string_index() {
    if (not entry<StringLiteral<S.len>(S)>::is_set) {
      record_string<S>();
    }
    return priv_get_string_index<S>();
  }

  index_type get_num_items() { return m_counter; }

  static index_type next_counter() { return m_counter++; }

  const static std::string &get_string(index_type index) {
    ASSERT_RELEASE(index < m_counter);
    return m_vec[index];
  }

  template <StringLiteral S>
  class entry {
   public:
    const static inline index_type id     = string_enumerator::next_counter();
    static inline bool             is_set = false;
  };

  static std::string get_string_by_index(const size_t index) {
    return m_vec[index];
  }

 private:
  template <StringLiteral S>
  static index_type priv_get_string_index() {
    return entry<StringLiteral<S.len>(S)>::id;
  }

  template <StringLiteral S>
  static void record_string() {
    ASSERT_RELEASE(entry<StringLiteral<S.len>(S)>::id < m_vec.size());
    m_vec[priv_get_string_index<S>()]      = S.value;
    entry<StringLiteral<S.len>(S)>::is_set = true;
  }

  static index_type m_counter;

  static std::vector<std::string> m_vec;
};

std::vector<std::string>      string_enumerator::m_vec;
string_enumerator::index_type string_enumerator::m_counter = 0;

string_enumerator::string_enumerator() {
  string_enumerator::m_vec.resize(string_enumerator::m_counter);
}
}  // namespace detail
}  // namespace ygm
