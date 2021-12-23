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

double compute_norm(
  ygm::container::map<boost::json::string, double> &pr_old, 
  ygm::container::map<boost::json::string, double> &pr_new,
  ygm::comm &world) {

  using map_type = ygm::container::map<boost::json::string, double>;

  static double local_norm_squared;
  local_norm_squared = 0.0;  

  auto compute_norm_lambda = [&pr_new](const auto &old_pr_pair) {

    auto &vtx_id = old_pr_pair.first;
    auto &pr_val = old_pr_pair.second; // old PR value.

    auto visit_new_pr = [](const auto &new_pr_pair, const auto &old_val) {

      auto &vtx_id = new_pr_pair.first;
      auto &new_val = new_pr_pair.second;

      auto diff = (new_val - old_val);
      diff = diff * diff; 
      local_norm_squared += diff; 
    };

    pr_new.async_visit(vtx_id, visit_new_pr, pr_val);
  };

  pr_old.for_all(compute_norm_lambda);
  world.barrier();

  auto global_norm_squared = sqrt(world.all_reduce_sum(local_norm_squared));
  //std::cout << "Static: " << local_norm_squared << " " << global_norm_squared << std::endl;

  return global_norm_squared;
}

int main(int argc, char **argv) {

  ygm::comm world(&argc, &argv);

  using map_type      = ygm::container::map<boost::json::string, double>;
  using maptrix_type  = ygm::container::experimental::maptrix<boost::json::string, double>;
  namespace ns_spmv   = ygm::container::experimental::detail::algorithms;

  std::cout.precision(4);

  map_type pr(world);
  map_type deg(world);
  maptrix_type A(world);
  
  if(argc == 1) {
    std::cout << "Expected parameter arguments, exiting.." << std::endl;
    exit(0);
  }

  std::vector<std::string> fnames;
  for (int i = 1; i < argc; ++i) {
    fnames.push_back(argv[i]);
  }

  auto A_acc_lambda = [](auto &row, auto &col, auto &value, const auto &update_val) {
    value = value + update_val;
  };

  auto deg_acc_lambda = [](auto &rv_pair, const auto &update_val) {
    rv_pair.second = rv_pair.second + update_val;
  };

  world.barrier();
  ygm::timer read_graph_timer{};

  // Building the author-author graph.
  ygm::io::ndjson_parser json_parser(world, fnames);
  json_parser.for_all([&A, &deg, &deg_acc_lambda, &A_acc_lambda](auto &json_line) {
    if (json_line["LL_author"] != "AutoModerator" &&
       json_line["LL_parent_author"] != "AutoModerator") {

      const auto &src = json_line["LL_author"].as_string(); 
      const auto &dst = json_line["LL_parent_author"].as_string();
      
      A.async_insert_if_missing_else_visit(src, dst, 1.0, A_acc_lambda, 1.0);
      deg.async_insert_if_missing_else_visit(dst, 1.0, deg_acc_lambda);

      A.async_insert_if_missing_else_visit(dst, src, 1.0, A_acc_lambda, 1.0);
      deg.async_insert_if_missing_else_visit(src, 1.0, deg_acc_lambda);
    }
  });

  world.barrier();
  double elapsed = read_graph_timer.elapsed();
  if (world.rank() == 0) {
    std::cout << "LOGGER: " << "Rank: " << world.rank()  
              << ", [MAX] Read graph time: " << elapsed << "s." << std::endl;
  }

  world.barrier();
  ygm::timer preprocess_timer{};

  double init_pr = 0.;
  auto acc_lambda = [&pr, &init_pr](auto &key) {
    pr.async_insert(key, init_pr);
  };
  A.for_all_row(acc_lambda);
  //A.for_all_col(acc_lambda); //Not reqd, undirected.
  if (world.rank() == 0) { 
    std::cout << "LOGGER: " << "Rank: " << world.rank() 
            << ", Step 2: " << "Created PageRank vector." << std::endl;
  }

  int N = pr.size();
  init_pr = ((float) 1)/N;
  if (world.rank() == 0) {
    std::cout << "LOGGER: " << "Rank: " << world.rank()
            << ", N: " << N << ", init PR: " <<  init_pr << "."<< std::endl;
  }

  auto mod_pr_lambda = [&init_pr](auto &rv_pair) {
    rv_pair.second = init_pr;
  };
  pr.for_all(mod_pr_lambda);
  if (world.rank() == 0) {
    std::cout << "LOGGER: " << "Rank: " << world.rank() 
            << ", Step 3: " << "Scaled PageRank values." << std::endl;
  }

  auto deg_lambda = [&A](auto &kv_pair) {
    auto vtx = kv_pair.first;
    auto deg = kv_pair.second;

    auto scale_A_lambda = [](const auto &row, const auto &col, auto &value, const auto &deg){
      value = ((float) value)/deg;
    };
    A.async_visit_col_mutate(vtx, scale_A_lambda, deg);
  };
  deg.for_all(deg_lambda);
  world.barrier();

  if (world.rank() == 0) {
    std::cout << "LOGGER: " << "Rank: " << world.rank() 
            << ", Step 4: " << "Scaled adjacency matrix values." << std::endl;
  }

  elapsed = preprocess_timer.elapsed();
  if (world.rank() == 0) {
    std::cout << "LOGGER: " << "Rank: " << world.rank()
              << ", [MAX] Preprocess graph time: " << elapsed << "s." << std::endl;
  }

  auto print_pr_lambda = [](auto &vtx_pr_pair) {
    std::cout << "[print pr] key: " << vtx_pr_pair.first << ", col: " << vtx_pr_pair.second << std::endl;
  };

  double agg_pr{0.};
  auto agg_pr_lambda = [&agg_pr](auto &vtx_pr_pair) {
    agg_pr = agg_pr + vtx_pr_pair.second;
  };
  pr.for_all(agg_pr_lambda);
  auto f_agg_pr = world.all_reduce_sum(agg_pr);
  if (world.rank() == 0) {
    std::cout << "LOGGER: " << "Rank: " << world.rank() 
            << ", Init iter: Agg PR: " << f_agg_pr << "." << std::endl;
  }

  agg_pr = 0.;
  double d_val = 0.85;
  double norm = 0.;
  double tol = 1e-6;

  //Add overall pagerank timer here.
  world.barrier();
  ygm::timer overall_pr_timer{};

  for (int iter = 0; iter < 100; iter++) {

    world.barrier();
    ygm::timer pr_timer{};

    auto map_res = ns_spmv::spmv(A, pr, std::plus<double>(), std::multiplies<double>());

    auto adding_damping_pr_lambda = [&map_res, &d_val, &N](auto &vtx_pr) {
      auto vtx_id = vtx_pr.first;
      auto pg_rnk = vtx_pr.second;

      auto visit_lambda = [] (auto &vtx_pr_pair, auto &da_val, auto &d_val) {
        vtx_pr_pair.second = da_val + d_val * vtx_pr_pair.second;
      };

      map_res.async_insert_if_missing_else_visit(vtx_id, (float (1-d_val)/N), visit_lambda, d_val);
    };
    pr.for_all(adding_damping_pr_lambda);
    world.barrier();  

    elapsed = pr_timer.elapsed();
    if (world.rank() == 0) {
      std::cout << "LOGGER: " << "Rank: " << world.rank() 
                << ", Iter [" << iter << "]: [MAX] PageRank compute time: " 
                << elapsed << "s." << std::endl;
    }

    norm = compute_norm(pr, map_res, world);
 
    pr.swap(map_res);

    //Aggregating overall PR values. 
    pr.for_all(agg_pr_lambda);
    f_agg_pr = world.all_reduce_sum(agg_pr);
    agg_pr = 0.; //Reset.
    pr_timer.reset();

    if (iter > 1 && norm < tol)
      break;
  }

  world.barrier();
  double overall_elapsed = overall_pr_timer.elapsed();
  if (world.rank() == 0) {
    std::cout << "LOGGER: " << "Rank: " << world.rank()
              << ", [MAX] Overall PageRank time: "
              << overall_elapsed << "s." << std::endl;
  }

  return 0;
}
