// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#include <iostream>

#include <ygm/comm.hpp>
#include <ygm/container/experimental/maptrix.hpp>
#include <ygm/container/map.hpp>

int main(int argc, char **argv) {
  ygm::comm world(&argc, &argv);

  using map_type = ygm::container::map<std::string, double>;
  using maptrix_type =
      ygm::container::experimental::maptrix<std::string, double>;
  namespace ns_spmv = ygm::container::experimental::detail::algorithms;

  map_type     pr(world);
  map_type     deg(world);
  maptrix_type A(world);

  if (argc == 1) {
    std::cout << "Expected parameter arguments, exiting.." << std::endl;
    exit(0);
  }

  std::string   fname = argv[1];
  std::ifstream matfile(fname);

  auto A_acc_lambda = [](auto &row, auto &col, auto &value,
                         const auto &update_val) {
    value = value + update_val;
  };

  auto deg_acc_lambda = [](auto &rv_pair, const auto &update_val) {
    rv_pair.second = rv_pair.second + update_val;
  };

  std::string key1, key2;
  if (world.rank0()) {
    while (matfile >> key1 >> key2) {
      // Makes maptrix symmetric
      A.async_insert_if_missing_else_visit(key1, key2, 1.0, A_acc_lambda, 1.0);
      deg.async_insert_if_missing_else_visit(key2, 1.0, deg_acc_lambda);

      A.async_insert_if_missing_else_visit(key2, key1, 1.0, A_acc_lambda, 1.0);
      deg.async_insert_if_missing_else_visit(key1, 1.0, deg_acc_lambda);
    }
  }

  double init_pr = 0.;
  std::cout << init_pr << std::endl;
  auto acc_lambda = [&pr, &init_pr](auto &key) {
    pr.async_insert(key, init_pr);
  };
  A.for_all_row(acc_lambda);

  int N              = pr.size();
  init_pr            = ((double)1) / N;
  auto mod_pr_lambda = [&init_pr](auto &rv_pair) { rv_pair.second = init_pr; };
  pr.for_all(mod_pr_lambda);

  auto deg_lambda = [&A](const auto &kv_pair) {
    auto vtx            = kv_pair.first;
    auto deg            = kv_pair.second;
    auto scale_A_lambda = [](const auto &row, const auto &col, auto &value,
                             const auto &deg) {
      value = ((double)value) / deg;
    };
    A.async_visit_col_mutate(vtx, scale_A_lambda, deg);
  };
  deg.for_all(deg_lambda);
  world.barrier();

  // Change pr vector based on degree and damping factor.
  double agg_pr{0.};
  double d_val = 0.85;
  for (int iter = 0; iter < 5; iter++) {
    /* Perform the SpMV operation here. */
    auto map_res =
        ns_spmv::spmv(A, pr, std::plus<double>(), std::multiplies<double>());
    world.barrier();

    auto adding_damping_pr_lambda = [&map_res, d_val, N](auto &vtx_pr) {
      auto vtx_id       = vtx_pr.first;
      auto pg_rnk       = vtx_pr.second;
      auto visit_lambda = [](auto &vtx_pr_pair, auto &da_val, auto &d_val) {
        vtx_pr_pair.second = da_val + d_val * vtx_pr_pair.second;
      };
      map_res.async_insert_if_missing_else_visit(vtx_id, (float(1 - d_val) / N),
                                                 visit_lambda, d_val);
    };
    pr.for_all(adding_damping_pr_lambda);
    pr.swap(map_res);

    auto agg_pr_lambda = [&agg_pr](auto &vtx_pr_pair) {
      agg_pr = agg_pr + vtx_pr_pair.second;
    };
    pr.for_all(agg_pr_lambda);
    world.barrier();
    world.all_reduce_sum(agg_pr);
    std::cout << "Aggregated PR: " << agg_pr << "." << std::endl;
    agg_pr = 0.;
  }

  return 0;
}
