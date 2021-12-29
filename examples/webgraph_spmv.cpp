// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#include <fstream>
#include <iostream>
#include <iomanip>

#include <math.h>
#include <ygm/comm.hpp>

#include <ygm/utility.hpp>

#include <ygm/container/map.hpp>
#include <ygm/container/experimental/maptrix.hpp>

#include <ygm/io/line_parser.hpp>
#include <ygm/io/ndjson_parser.hpp>

#include <boost/tokenizer.hpp>

int main(int argc, char **argv) {

  std::cout << std::fixed;
  std::cout << std::setprecision(8);

  ygm::comm world(&argc, &argv);

  //using map_type      = ygm::container::map<ssize_t, int>;
  //using maptrix_type  = ygm::container::experimental::maptrix<ssize_t, int>;
  
  using map_type      = ygm::container::map<int32_t, int>;
  using maptrix_type  = ygm::container::experimental::maptrix<int32_t, int>;

  namespace ns_spmv   = ygm::container::experimental::detail::algorithms;

  map_type x(world);
  maptrix_type A(world);
  
  auto x_ptr = x.get_ygm_ptr();
  auto A_ptr = A.get_ygm_ptr(); 

  if(argc == 1) {
    std::cout << "Expected parameter arguments, exiting.." << std::endl;
    exit(0);
  }

  //std::string m_name = argv[1];
  //std::string v_name = argv[2];

  std::vector<std::string> fnames;
  for (int i = 1; i < argc; ++i) {
    std::cout << argv[i] << std::endl;
    fnames.push_back(argv[i]);
  }

  ygm::io::line_parser line_parser(world, fnames);
  //ssize_t src, dst; 
  int32_t src, dst; 
  line_parser.for_all([&A, &x, &src, &dst](auto &line) {
    std::istringstream iss(line);
    if (iss >> src >> dst) {
      //std::cout << src << " " << dst << std::endl;
      /* Maptrix A. */
      A.async_insert(src, dst, 1);
      A.async_insert(dst, src, 1);

      /* Map x. */
      // You might want to use rand here.
      x.async_insert(src, 1);
      x.async_insert(dst, 1);
    }
  });

  #ifdef dbg
  auto ijk_lambda = [&A](const auto &row, const auto &col, const auto &value) {
    auto &mptrx_comm = A.comm();
    int rank         = mptrx_comm.rank();
    std::cout << "[MPTRX]: In rank: " << rank << ", key1: " 
              << row << ", key2: " << col << ", val: " << value << std::endl;
  };
  A.for_all(ijk_lambda);
  world.barrier();

  auto map_lambda = [](const auto &res_kv_pair) {
    std::cout << "[In map lambda] key: " << res_kv_pair.first << ", col: " << res_kv_pair.second << std::endl;
  };
  x.for_all(map_lambda);
  world.barrier();
  #endif
 
  world.barrier();
  ygm::timer spmv_timer{};
  
  auto y = ns_spmv::spmv(A, x, std::plus<double>(), std::multiplies<double>());

  world.barrier();
  double elapsed = spmv_timer.elapsed();
  if (world.rank() == 0) {
    std::cout << "LOGGER: " << "Rank: " << world.rank()
              << ", [MAX] SpMV time: " << elapsed << "s." << std::endl;
  }

  return 0;
}
