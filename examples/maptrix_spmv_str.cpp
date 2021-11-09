// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#include <ygm/comm.hpp>
#include <ygm/container/map.hpp>
#include <ygm/container/assoc_vector.hpp>
#include <ygm/container/maptrix.hpp>

int main(int argc, char **argv) {

  ygm::comm world(&argc, &argv);

  using maptrix_type = ygm::container::maptrix<std::string, std::string>;
  using map_type = ygm::container::assoc_vector<std::string, std::string>;
  maptrix_type my_maptrix(world);
  /* Insert entries of the maptrix.. */

  if (world.rank0()) {
    my_maptrix.async_insert("0", "0", "a");
    my_maptrix.async_insert("0", "1", "b");
    my_maptrix.async_insert("0", "2", "c");
    my_maptrix.async_insert("0", "3", "d");

    my_maptrix.async_insert("1", "0", "e");
    my_maptrix.async_insert("1", "1", "f");
    my_maptrix.async_insert("1", "2", "g");
    my_maptrix.async_insert("1", "3", "h");

    my_maptrix.async_insert("2", "0", "i");
    my_maptrix.async_insert("2", "1", "j");
    my_maptrix.async_insert("2", "2", "k");
    my_maptrix.async_insert("2", "3", "l");
  }

  /* Behaves as the vector for SpMV.. */
  //ygm::container::map<std::string, std::string> my_map(world);
  map_type my_map(world);
  if (world.rank0()) {
    my_map.async_insert("0", "0");
    my_map.async_insert("1", "1");
    my_map.async_insert("2", "2");
    my_map.async_insert("3", "3");
  }

  /* Behaves as the vector for SpMV.. */
  map_type map_res(world);
  auto map_res_ptr = map_res.get_ygm_ptr();

  auto ijk_lambda = [&my_maptrix](auto row, auto col, auto value) {
    auto &mptrx_comm = my_maptrix.comm();
    int rank         = mptrx_comm.rank();

    std::cout << "[MPTRX]: In rank: " << rank << 
                 ", key1: " << row << 
                 ", key2: " << col << 
                 ", val: " << value << std::endl;
  };
  my_maptrix.for_all(ijk_lambda);
  world.barrier();

  auto print_kv_lambda = [&map_res](auto res_kv_pair) {
    std::cout << "[MAP]: In rank: " << map_res.comm().rank() << 
                 " key: " << res_kv_pair.first << 
                 ", col: " << res_kv_pair.second << std::endl;  
  };
  my_map.for_all(print_kv_lambda);
  world.barrier();

  auto kv_lambda = [&my_maptrix, map_res_ptr](auto kv_pair) {
    auto &mptrx_comm = my_maptrix.comm();
    int rank         = mptrx_comm.rank();

    auto col        = kv_pair.first; 
    auto col_value  = kv_pair.second; 

    auto maptrix_visit_lambda = [](auto col, auto row, auto mat_value, auto vec_value, auto map_res_ptr) {
      auto element_wise = mat_value.append(vec_value); 

      auto append_lambda = [](auto row_id_val, auto update_val) {
        auto row_id = row_id_val->first; 
        auto value =  row_id_val->second;
        auto append_val = value.append(update_val);
        row_id_val->second = row_id_val->second.append(update_val);
      };

      map_res_ptr->async_visit_or_insert(row, element_wise, append_lambda, element_wise);
    };

    my_maptrix.async_visit_col_const(col, maptrix_visit_lambda, col_value, map_res_ptr);
  };

  my_map.for_all(kv_lambda);
  world.barrier();

  map_res.for_all(print_kv_lambda);
  world.barrier();

  return 0;
}
