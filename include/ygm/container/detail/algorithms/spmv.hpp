// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once

#include <map>
#include <fstream>
#include <ygm/comm.hpp>
#include <ygm/detail/ygm_ptr.hpp>
#include <cereal/archives/json.hpp>
#include <cereal/types/utility.hpp>
#include <ygm/container/assoc_vector.hpp>
#include <ygm/container/detail/adj_impl.hpp>
#include <ygm/container/detail/hash_partitioner.hpp>

namespace ygm::container::detail::algorithms {

  //using key_type    = Key;
  //using value_type  = Value;
  using map_type    = ygm::container::assoc_vector<key_type, value_type>;
  //using adj_impl    = detail::adj_impl<key_type, value_type, Partitioner, Compare, Alloc>;
  //using inner_map_type  = std::map<Key, Value>;

  map_type spmv(map_type& x) {
    
    map_type y(m_comm);
    auto y_ptr = y.get_ygm_ptr();
    auto A_ptr = pthis->get_ygm_ptr();
    
    auto kv_lambda = [A_ptr, y_ptr](auto kv_pair) {
      
      auto &mptrx_comm = A_ptr->comm();
      int rank         = mptrx_comm.rank();
      
      auto col        = kv_pair.first;
      auto col_value  = kv_pair.second;
      
      auto csc_visit_lambda = [](
        auto col, auto row, 
        auto A_value, auto x_value, auto y_ptr) {
        
        auto element_wise = A_value * x_value;
        
        auto append_lambda = [](auto rv_pair, auto update_val) {
          auto row_id = rv_pair->first;
          auto value  = rv_pair->second;
          auto append_val = value + update_val;
          rv_pair->second = rv_pair->second + update_val;
        };
        
        y_ptr->async_visit_or_insert(row, element_wise, append_lambda, element_wise);
      };
      
      A_ptr->async_visit_col_const(col, csc_visit_lambda, col_value, y_ptr);
    };

    x.for_all(kv_lambda);
    m_comm.barrier();

    return y;
  }

} // namespace ygm::container::detail
