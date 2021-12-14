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

#include <ygm/container/map.hpp>
#include <ygm/container/experimental/maptrix.hpp>

namespace ygm::container::experimental::detail::algorithms {

  //Create times op.

  //template <typename Key, typename Value, typename OpPlus, typename OpMultiply>
  template <typename Key, typename Value>
  ygm::container::map<Key, Value> spmv_row(
    ygm::container::experimental::maptrix<Key, Value> &A, ygm::container::map<Key, Value> &x) {
    //ygm::container::maptrix<Key, Value> &A, ygm::container::map<Key, Value> &x,
    //    OpPlus plus_op=std::plus<Value>(), OpMultiply times_op=std::times) {
    
    using key_type    = Key;
    using value_type  = Value;
    using map_type    = ygm::container::map<key_type, value_type>;

    auto A_ptr = A.get_ygm_ptr();
    auto x_ptr = x.get_ygm_ptr();
    auto A_comm = A_ptr->comm();

    map_type y(A_comm);
    auto y_ptr = y.get_ygm_ptr();

    // 1: initialize y values to 0. 
    auto init_y = [&y_ptr](auto xval){
      auto row_y = xval.first;
      y_ptr->insert(row_y, 0);
    };
     
    // 2: loop over all entries of y. 
    auto y_lambda = [&A_ptr, &x_ptr](auto yval){

      auto &col = yval.first; 
      auto &acc = yval.second;

      auto x_lambda = [](const auto xval, auto &agg, auto &A_ptr) {
        auto &x = xval.first; 
        auto &x_val = xval.second; 

        A_lambda = [&agg, &col, &col_val](auto row, auto col, auto val){
           
        };

        A_ptr->for_all_row(A_lambda);
      }; 
      x_ptr->async_visit(col, x_lambda, acc, A_ptr);     
    }; 

    y.for_all(y_lambda);

    return y;
  }

} // namespace ygm::container::experimental::detail::algorithms
