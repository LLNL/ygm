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

  map_type pr(world);
  map_type deg(world);
  maptrix_type A(world);
  
  auto pr_ptr  = pr.get_ygm_ptr();
  auto deg_ptr = deg.get_ygm_ptr();
  auto A_ptr   = A.get_ygm_ptr(); 

  std::ifstream matfile("/g/g90/tom7/codebase/intern_2021/data/pr_small.graph");

  auto deg_acc_lambda = [](auto &rv_pair, const auto &update_val) {
    rv_pair.second = rv_pair.second + update_val;
  };

  std::string key1, key2;
  if (world.rank0()) {
    while (matfile >> key1 >> key2) {
      A.async_insert(key1, key2, 1.0);
      //pr.async_insert(key1, 0.25);
      //pr.async_insert(key2, 0.25);
      ////deg.async_insert_if_missing_else_visit(key1, 1.0, deg_acc_lambda);
      deg.async_insert_if_missing_else_visit(key2, 1.0, deg_acc_lambda);
    }
  }

  double init_pr = 0.;
  std::cout << init_pr << std::endl;
  auto acc_lambda = [&pr, &init_pr](auto &key) {
    pr.async_insert(key, init_pr);
  };
  A.for_all_row(acc_lambda);
  A.for_all_col(acc_lambda);

  int N = pr.size();
  init_pr = ((float) 1)/N;
  auto mod_pr_lambda = [&init_pr](auto &rv_pair) {
    rv_pair.second = init_pr;
  };
  pr.for_all(mod_pr_lambda);

  #ifdef for_all_edges
  auto outer_lambda = [&deg](auto &key) {
    auto deg_acc_lambda = [](auto &rv_pair, const auto &update_val) {
      rv_pair.second = rv_pair.second + update_val;
    };
    deg.async_insert_if_missing_else_visit(key, 1.0, deg_acc_lambda);
  };
  A.for_all_col(outer_lambda);
  #endif
  
  auto ijk_lambda = [&A](auto row, auto col, auto value) {
    auto &mptrx_comm = A.comm();
    int rank         = mptrx_comm.rank();
    std::cout << "[MPTRX]: In rank: " << rank << ", key1: " << row << ", key2: " << col << ", val: " << value << std::endl;
  };

  auto map_lambda = [](auto res_kv_pair) {
    std::cout << "[In map lambda] key: " << res_kv_pair.first << ", col: " << res_kv_pair.second << std::endl;
  };

  #ifdef abc
  A.for_all(ijk_lambda);
  world.barrier();
  pr.for_all(map_lambda);
  world.barrier();
  deg.for_all(map_lambda);
  world.barrier();
  #endif

  auto deg_lambda = [&A_ptr](auto &kv_pair) {
    auto vtx = kv_pair.first;
    auto deg = kv_pair.second;

    auto scale_A_lambda = [](const auto &row, const auto &col, auto &value, const auto &deg){
      //auto norm_val = ((float) value)/deg;
      //norm_A_ptr->async_insert(col, row, norm_val);
      value = ((float) value)/deg;
      std::cout << "Inside scale lambda: " << value << std::endl;
    };
    A_ptr->async_visit_col_mutate(vtx, scale_A_lambda, deg);
  };

  deg.for_all(deg_lambda);
  world.barrier();

  A.for_all(ijk_lambda);
  world.barrier();

  auto print_pr_lambda = [](auto &vtx_pr_pair) {
    std::cout << "[print pr] key: " << vtx_pr_pair.first << ", col: " << vtx_pr_pair.second << std::endl;
  };

  // Change pr vector based on degree and damping factor. 
  double d_val = 0.85;
  for (int iter = 0; iter < 20; iter++) {

    /* Perform the SpMV operation here. */
    auto map_res = ns_spmv::spmv(A, pr);
    //map_res.for_all(print_pr_lambda);
    //world.barrier();

    auto map_res_ptr = map_res.get_ygm_ptr();

    auto adding_damping_pr_lambda = [map_res_ptr, d_val, N](auto &vtx_pr) {
      auto vtx_id = vtx_pr.first;
      auto pg_rnk = vtx_pr.second;
      auto visit_lambda = [] (auto &vtx_pr_pair, auto &da_val, auto &d_val) {
        vtx_pr_pair.second = da_val + d_val * vtx_pr_pair.second;
      };
      map_res_ptr->async_insert_if_missing_else_visit(vtx_id, (float (1-d_val)/N), visit_lambda, d_val);
    };
    pr.for_all(adding_damping_pr_lambda);
    pr.swap(map_res);

    std::cout << "After update: " << std::endl;
    pr.for_all(print_pr_lambda);
    world.barrier();
  }

  return 0;
}
