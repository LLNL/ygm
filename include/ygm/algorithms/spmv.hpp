// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// // Project Developers. See the top-level COPYRIGHT file for details.
// //
// // SPDX-License-Identifier: MIT

#pragma once

#include <ygm/algorithms/detail/spmv_impl.hpp>

namespace ygm::algorithms {

template <typename Key, typename Value>
class spmv {
 public:
  using key_type   = Key;
  using value_type = Value;
  using self_type  = spmv<Key, Value>;
  using impl_type  =
      detail::spmv_impl<key_type, value_type>;

  using map_type = ygm::container::assoc_vector<std::string, double>;
  using maptrix_type = ygm::container::maptrix<std::string, double>;

  spmv() = delete;

  spmv(ygm::comm& comm) : m_impl(comm) {}

  map_type spmv_op(maptrix_type& my_maptrix, map_type& my_map) {
    return m_impl.spmv_op(my_maptrix, my_map);
  }

  /* Do we really need this? */
  typename ygm::ygm_ptr<impl_type> get_ygm_ptr() const {
    return m_impl.get_ygm_ptr();
  }

 private:
  impl_type m_impl;
};
}  // namespace ygm::container
