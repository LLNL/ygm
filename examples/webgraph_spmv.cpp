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

  using map_type      = ygm::container::map<std::string, double>;
  using maptrix_type  = ygm::container::experimental::maptrix<std::string, double>;
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
  line_parser.for_all([&A, &x](auto &line) {

    boost::char_separator<char> sep(" ");
    boost::tokenizer<boost::char_separator<char>> tokens(line, sep);
    std::vector<std::string> vtx_ids;
    for (const auto &t : tokens) {
      std::string b_t{t};
      vtx_ids.push_back(b_t);
    }

    const auto &src = vtx_ids.at(0);
    const auto &dst = vtx_ids.at(1);

    /* Maptrix A. */
    A.async_insert(src, dst, 1.0);
    A.async_insert(dst, src, 1.0);

    /* Map x. */
    // You might want to use rand here.
    x.async_insert(src, 1.0);
    x.async_insert(dst, 1.0);
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
