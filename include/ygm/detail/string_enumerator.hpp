// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once

#include <algorithm>
#include <array>
#include <iostream>
#include <vector>

template <size_t N> struct StringLiteral {
  constexpr StringLiteral(const char (&str)[N]) { std::copy_n(str, N, value); }

  char value[N];
};

class string_enumerator {
public:
public:
  template <StringLiteral S> class entry {
  public:
    const static int id;
    static inline bool is_set = false;
  };

  string_enumerator();

  template <StringLiteral S> static int get_string_index() {
    if (not entry<S>::is_set) {
      record_string<S>();
    }
    return priv_get_string_index<S>();
  }

  int get_num_items() { return m_counter; }

  static int next_counter() { return m_counter++; }

  template <StringLiteral S>
  static std::string get_string() {
    return m_vec[get_string_index<S>()];
  }

private:

  template <StringLiteral S>
  static int priv_get_string_index() {
    return entry<S>::id;
  }

  template <StringLiteral S>
  static void record_string() {
    ASSERT_RELEASE(entry<S>::id < m_vec.size());
    m_vec[priv_get_string_index<S>()] = S.value;
    entry<S>::is_set = true;
  }

  static int m_counter;
public:
  static std::vector<std::string> m_vec;
};

std::vector<std::string> string_enumerator::m_vec;
int string_enumerator::m_counter = 0;
template <StringLiteral S>
const int string_enumerator::entry<S>::id = string_enumerator::next_counter();

string_enumerator::string_enumerator() {
  string_enumerator::m_vec.resize(string_enumerator::m_counter);
}
