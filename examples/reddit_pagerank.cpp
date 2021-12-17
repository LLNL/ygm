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
#include <ygm/io/ndjson_parser.hpp>
#include <ygm/container/experimental/maptrix.hpp>

#include <ygm/utility.hpp>

void compute_norm(
  ygm::container::map<boost::json::string, double> &pr_old, 
  ygm::container::map<boost::json::string, double> &pr_new,
  ygm::container::map<std::string, double> norm_map,
  ygm::comm &world) {

  using map_type = ygm::container::map<std::string, double>;

  int N_old = pr_old.size();
  int N_new = pr_new.size();
  //std::cout << "Prev iter size: " << N_old << ", Current iter size: " << N_new << std::endl;

  auto pr_old_ptr = pr_old.get_ygm_ptr();
  auto pr_new_ptr = pr_new.get_ygm_ptr();

  //map_type norm_map(world);
  norm_map.async_insert(std::string("dist"), 0);
  auto norm_map_ptr = norm_map.get_ygm_ptr();

  auto compute_norm_lambda = [&pr_old_ptr, &pr_new_ptr, &norm_map_ptr](auto old_pr_pair) {

    //norm_acc += 1;
    auto vtx_id = old_pr_pair.first;
    auto pr_val = old_pr_pair.second; // old PR value.

    auto visit_new_pr = [](auto &new_pr_pair, const auto old_val, auto &norm_map_ptr) {

      auto vtx_id = new_pr_pair.first;
      auto new_val = new_pr_pair.second;

      auto diff = (new_val - old_val);
      diff = diff * diff;
      //norm_acc = diff + 10;

      //norm_acc = norm_acc + diff;
      //std::cout << vtx_id << " " << old_val << " " << new_val << std::endl;
      //std::cout << norm_acc << " " << diff << std::endl;

      auto accumulate_lambda = [](auto &row_id_val, const auto &update_val) {
        auto row_id = row_id_val.first;
        auto value =  row_id_val.second;
        auto append_val = value + update_val;
        row_id_val.second = row_id_val.second + update_val;
      };

      norm_map_ptr->async_insert_if_missing_else_visit(std::string("dist"), diff, accumulate_lambda);
    };

    pr_new_ptr->async_visit(vtx_id, visit_new_pr, pr_val, norm_map_ptr);
  };

  pr_old_ptr->for_all(compute_norm_lambda);
  world.barrier();

  //double global_norm_acc; 
  //double global_norm_acc = world.all_reduce_sum(norm_acc);
  //std::cout << global_norm_acc;
  auto print_res_lambda = [](auto res_kv_pair) {
    std::cout << "Resulting norm: "  << sqrt(res_kv_pair.second) << std::endl;
  };
  norm_map.async_visit(std::string("dist"), print_res_lambda);
}

int main(int argc, char **argv) {

  ygm::comm world(&argc, &argv);

  using map_type      = ygm::container::map<boost::json::string, double>;
  using maptrix_type  = ygm::container::experimental::maptrix<boost::json::string, double>;
  namespace ns_spmv   = ygm::container::experimental::detail::algorithms;

  //using map_type      = ygm::container::map<std::string, double>;
  //using maptrix_type  = ygm::container::experimental::maptrix<std::string, double>;
  //namespace ns_spmv   = ygm::container::experimental::detail::algorithms;

  map_type pr(world);
  map_type deg(world);
  maptrix_type A(world);
  
  auto pr_ptr  = pr.get_ygm_ptr();
  auto deg_ptr = deg.get_ygm_ptr();
  auto A_ptr   = A.get_ygm_ptr(); 

  auto A_acc_lambda = [](auto &row, auto &col, auto &value, const auto &update_val) {
    value = value + update_val;
  };

  auto deg_acc_lambda = [](auto &rv_pair, const auto &update_val) {
    rv_pair.second = rv_pair.second + update_val;
  };

  #ifdef abc
  auto fname = argv[0];
  std::ifstream matfile();
  std::string key1, key2;
  if (world.rank0()) {
    while (matfile >> key1 >> key2) {
      A.async_insert(key1, key2, 1.0);
      deg.async_insert_if_missing_else_visit(key2, 1.0, deg_acc_lambda);
    }
  }
  #endif

  ygm::timer read_graph_timer{};

  std::vector<std::string> fnames;
  for (int i = 1; i < argc; ++i) {
    fnames.push_back(argv[i]);
    std::cout << argv[i];
  }

  // Building the author-author graph.
  ygm::io::ndjson_parser json_parser(world, fnames);
  json_parser.for_all([&A, &deg, &deg_acc_lambda, &A_acc_lambda](auto &json_line) {
    if (json_line["LL_author"] != "AutoModerator" &&
       json_line["LL_parent_author"] != "AutoModerator") {

      /* Reading it in backward.. */
      auto dst = json_line["LL_author"].as_string(); 
      auto src = json_line["LL_parent_author"].as_string();
      
      A.async_insert_if_missing_else_visit(src, dst, 1.0, A_acc_lambda, 1.0);
      deg.async_insert_if_missing_else_visit(dst, 1.0, deg_acc_lambda);

      A.async_insert_if_missing_else_visit(dst, src, 1.0, A_acc_lambda, 1.0);
      deg.async_insert_if_missing_else_visit(src, 1.0, deg_acc_lambda);
    }
  });

  std::cout << "LOGGER: Step 1: " << "Finished reading in the author-author graph." << std::endl; 

  double elapsed = read_graph_timer.elapsed();
  std::cout << "Read graph time: " << elapsed << std::endl;

  ygm::timer preprocess_timer{};

  double init_pr = 0.;
  auto acc_lambda = [&pr, &init_pr](auto &key) {
    pr.async_insert(key, init_pr);
  };
  A.for_all_row(acc_lambda);
  A.for_all_col(acc_lambda); 
  std::cout << "LOGGER: Step 2: " << "Created pagerank vector." << std::endl;

  int N = pr.size();
  init_pr = ((float) 1)/N;
  std::cout << "N: " << N << ", init pr: " <<  init_pr << std::endl;
  auto mod_pr_lambda = [&init_pr](auto &rv_pair) {
    rv_pair.second = init_pr;
  };
  pr.for_all(mod_pr_lambda);
  std::cout << "LOGGER: Step 3: " << "Scaled pagerank values." << std::endl;

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
      //std::cout << "Inside scale lambda: " << row << " " << col << " " << value << std::endl;
      value = ((float) value)/deg;
    };
    A_ptr->async_visit_col_mutate(vtx, scale_A_lambda, deg);
  };

  std::cout << "LOGGER: Step 4: " << "Scaled adjacency matrix values." << std::endl;

  deg.for_all(deg_lambda);
  world.barrier();

  //deg.for_all(map_lambda);
  //world.barrier();
  //A.for_all(ijk_lambda);
  //world.barrier(); 

  elapsed = preprocess_timer.elapsed();
  std::cout << "Preprocess time: " << elapsed << std::endl;
 
  auto print_pr_lambda = [](auto &vtx_pr_pair) {
    std::cout << "[print pr] key: " << vtx_pr_pair.first << ", col: " << vtx_pr_pair.second << std::endl;
  };

  ygm::timer pr_timer{};

  double agg_pr{0.};
  auto agg_pr_lambda = [&agg_pr](auto &vtx_pr_pair) {
    //std::cout << agg_pr << " " << vtx_pr_pair.second << std::endl;
    agg_pr = agg_pr + vtx_pr_pair.second;
  };
  pr.for_all(agg_pr_lambda);
  auto f_agg_pr = world.all_reduce_sum(agg_pr);
  std::cout << "LOGGER: " << "Aggregated PR: " << f_agg_pr << "." << std::endl;

  ygm::container::map<std::string, double> norm_map(world);
  //auto norm_map_ptr = norm_map.get_ygm_ptr();

  agg_pr = 0.;
  double d_val = 0.85;
  for (int iter = 0; iter < 100; iter++) {

    auto map_res = ns_spmv::spmv(A, pr);
    auto map_res_ptr = map_res.get_ygm_ptr();

    auto adding_damping_pr_lambda = [map_res_ptr, d_val, N](auto &vtx_pr) {
      auto vtx_id = vtx_pr.first;
      auto pg_rnk = vtx_pr.second;
      //std::cout << "vtx id: " << vtx_id << ", pg rank: " << pg_rnk << std::endl;
      auto visit_lambda = [] (auto &vtx_pr_pair, auto &da_val, auto &d_val) {
        vtx_pr_pair.second = da_val + d_val * vtx_pr_pair.second;
      };
      map_res_ptr->async_insert_if_missing_else_visit(vtx_id, (float (1-d_val)/N), visit_lambda, d_val);
    };
    pr.for_all(adding_damping_pr_lambda);
    world.barrier(); //Does the map already have a barrier? 

    compute_norm(pr, map_res, norm_map, world);
    //auto print_res_lambda = [](auto res_kv_pair) {
      //std::cout << "Resulting: " << " key: " << res_kv_pair.first 
                //<< ", col: " << sqrt(res_kv_pair.second) << std::endl;
    //};
    //norm_map.async_visit(std::string("dist"), print_res_lambda);
 
    pr.swap(map_res);

    //std::cout << "After update: " << std::endl;
    //pr.for_all(print_pr_lambda);

    //Aggregating overall PR values. 
    pr.for_all(agg_pr_lambda);
    f_agg_pr = world.all_reduce_sum(agg_pr);
    //std::cout << "LOGGER: " << "Individual PR: " << agg_pr << "." << std::endl;
    std::cout << "LOGGER: " << "Completed Iter: " << iter 
              << ", Aggregated PR: " << f_agg_pr << "." << std::endl;

    agg_pr = 0.; //Reset.

    elapsed = pr_timer.elapsed();
    std::cout << "Iter time: " << elapsed << std::endl;
    pr_timer.reset();
  }

  //std::cout << "After update: " << std::endl;
  //pr.for_all(print_pr_lambda);
  return 0;
}
