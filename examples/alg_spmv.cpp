// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#include <fstream>
#include <iostream>
#include <iomanip>

#include <math.h>
#include <ygm/comm.hpp>

#include <ygm/container/map.hpp>
#include <ygm/container/experimental/maptrix.hpp>

int main(int argc, char **argv) {

  ygm::comm world(&argc, &argv);

  using map_type      = ygm::container::map<std::string, double>;
  using maptrix_type  = ygm::container::experimental::maptrix<std::string, double>;
  namespace ns_spmv   = ygm::container::experimental::detail::algorithms;

  map_type my_map(world);
  maptrix_type my_maptrix(world);
  
  auto my_map_ptr     = my_map.get_ygm_ptr();
  auto my_maptrix_ptr = my_maptrix.get_ygm_ptr(); 

  if(argc == 1) {
    std::cout << "Expected parameter arguments, exiting.." << std::endl;
    exit(0);
  }

  std::string m_name = argv[1];
  std::string v_name = argv[2];

  std::ifstream matfile(m_name);
  std::ifstream vecfile(v_name);
  double value;
  std::string key1, key2;
  if (world.rank0()) {

    while (matfile >> key1 >> key2 >> value) {
      my_maptrix.async_insert(key1, key2, value);
    }

    while (vecfile >> key1 >> value) {
      my_map.async_insert(key1, value);
    }
  }

  #ifdef dbg
  auto ijk_lambda = [&my_maptrix](const auto &row, const auto &col, const auto &value) {
    auto &mptrx_comm = my_maptrix.comm();
    int rank         = mptrx_comm.rank();
    std::cout << "[MPTRX]: In rank: " << rank << ", key1: " 
              << row << ", key2: " << col << ", val: " << value << std::endl;
  };
  my_maptrix.for_all(ijk_lambda);
  world.barrier();

  auto map_lambda = [](const auto &res_kv_pair) {
    std::cout << "[In map lambda] key: " << res_kv_pair.first << ", col: " << res_kv_pair.second << std::endl;
  };
  my_map.for_all(map_lambda);
  world.barrier();
  #endif
  
  auto times_op = ns_spmv::times<double>();
  auto map_res  = ns_spmv::spmv(my_maptrix, my_map, std::plus<double>(), std::multiplies<double>());

  #ifdef dbg
  auto print_res_lambda = [](auto res_kv_pair) {
    std::cout << "[In map res lambda] key: " << res_kv_pair.first << 
                  ", col: " << res_kv_pair.second << std::endl;
  };
  map_res.for_all(print_res_lambda);
  world.barrier();
  #endif

  std::cout << std::fixed;
  std::cout << std::setprecision(8);

  map_type map_gt(world);
  std::string gt_fname = argv[3];
  std::ifstream gtfile(gt_fname);

  if (world.rank0()) {
    while (gtfile >> key1 >> value) {
      map_gt.async_insert(key1, value);
    }
  }

  map_type norm_map(world);
  norm_map.async_insert(std::string("dist"), 0);

  auto gt_ptr       = map_gt.get_ygm_ptr();
  auto norm_map_ptr = norm_map.get_ygm_ptr();
  auto check_res_lambda = [norm_map_ptr, gt_ptr](auto res_kv_pair) {

    auto res_key = res_kv_pair.first;
    auto res_val = res_kv_pair.second;

    auto visit_ground = [](auto gt_pair, auto res_val, auto norm_map_ptr) {

      auto gt_key = gt_pair.first;
      auto gt_val = gt_pair.second;

      auto diff = (gt_val - res_val);
      diff = diff * diff;

      auto accumulate_lambda = [](auto &row_id_val, const auto &update_val) {
        auto row_id = row_id_val.first;
        auto value =  row_id_val.second;
        auto append_val = value + update_val;
        row_id_val.second = row_id_val.second + update_val;
      };

      norm_map_ptr->async_insert_if_missing_else_visit(std::string("dist"), diff, accumulate_lambda);
    };

    gt_ptr->async_visit(res_key, visit_ground, res_val, norm_map_ptr);
  };
  map_res.for_all(check_res_lambda);
  world.barrier();

  //if (world.rank0()) {
    auto print_res_lambda = [&norm_map_ptr](auto res_kv_pair) {
      auto &comm = norm_map_ptr->comm();
      auto irank = comm.rank();
      std::cout << "[In map res lambda] rank: " << irank << " key: " << res_kv_pair.first << ", col: " << sqrt(res_kv_pair.second) << std::endl;
    };
    norm_map.for_all(print_res_lambda);
  //}

  return 0;
}
