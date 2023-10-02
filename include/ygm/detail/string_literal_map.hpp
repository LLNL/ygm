// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#include <ygm/detail/string_enumerator.hpp>

namespace ygm {
namespace detail {

// Flat-ish map with string-literal keys which are enumerated at static initialization
// time with ygm::detail::string_enumerator. Because the string_enumerator
// enumerates keys for all maps, each string_literal_map will
// have an entry for every enumerated string, necessitating the bit
// vector to determine which entries are valid in each string_literal_map.
template <typename Value>
class string_literal_map {
 public:
  using mapped_type = Value;

  // class iterator;

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
  bool is_filled() { return m_key_mask[m_enumerator.get_string_index<S>()]; }

  bool is_filled(size_t index) { return m_key_mask[index]; }

  // iterator begin() const;
  // iterator end() const;

 private:
  std::vector<mapped_type> m_values;
  std::vector<bool>        m_key_mask;

 public:
  string_enumerator m_enumerator;
};

/*
template <typename Key, typename Value>
class constexpr_key_map::iterator {
        public:
                using key_type = Key;
                using mapped_type = Value;

                iterator() : p_map(nullptr), m_index(0) {};

                iterator(size_t index, const constexpr_key_map<Key, Value>
*map_ptr) : m_index(index), p_map(map_ptr) {};

                iterator& operator++() {++m_index;}
                iterator operator++(int n) {

        private:
                void update_iterator() {
                        m_k = p_map->m_enumerator.get_item(m_index);
                        m_v = GET_VALUE(*p_map
                }

                size_t m_index;
                const constexpr_key_map<Key, Value> *p_map;

                key_type &m_k;
                mapped_type &m_v;
};
*/

}  // namespace detail
}  // namespace ygm
