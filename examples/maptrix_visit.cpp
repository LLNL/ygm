// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#include <ygm/comm.hpp>
#include <ygm/container/maptrix.hpp>

int main(int argc, char **argv) {

  ygm::comm world(&argc, &argv);

  ygm::container::maptrix<std::string, std::string> my_maptrix(world);
  //std::cout << "Size: " << world.size();
  
  #ifdef random_map_check
  using inner_map_type = std::map<std::string, std::string>;
  std::map<std::string, inner_map_type> m_row_map;   

  m_row_map["row1"].insert(std::make_pair("row1001", "val1"));
  std::cout << m_row_map["row1"]["row1001"] << std::endl;

  m_row_map["row1"].insert(std::make_pair("row1001", "test"));
  auto inner_map = m_row_map["row1"];
  for (auto itr = inner_map.begin(); itr != inner_map.end(); ++itr) {
    std::cout << itr->first << " " << itr->second << std::endl;
  }

  m_row_map["row1"]["row1001"] = "test";
  std::cout << m_row_map["row1"]["row1001"] << std::endl;
  exit(0);
  #endif

  if (world.rank0()) {
    my_maptrix.async_insert("row1", "row2", "val1");
    my_maptrix.async_insert("row1", "row3", "val7");
    //my_maptrix.async_insert("row1", "row1001", "val8");
    
    my_maptrix.async_insert("row2", "row3",     "val2");
    my_maptrix.async_insert("row2", "row1003",  "val15");

    my_maptrix.async_insert("row3", "row1002", "val3");
    my_maptrix.async_insert("row3", "row1001", "val13");

    my_maptrix.async_insert("row1001", "row1", "val10");
    my_maptrix.async_insert("row1001", "row2", "val11");
    my_maptrix.async_insert("row1001", "row1002", "val4");

    my_maptrix.async_insert("row1002", "row2", "val12");
    my_maptrix.async_insert("row1002", "row1003", "val5");

    my_maptrix.async_insert("row1003", "row3", "val14");
    my_maptrix.async_insert("row1003", "row1001", "val6");
  }

  world.barrier();

  /* What maptrix object is this?? */
  auto ijk_lambda = [&my_maptrix](auto row, auto col, auto value) {
    auto &mptrx_comm = my_maptrix.comm();
    int rank         = mptrx_comm.rank();
    std::cout << "In rank: " << rank << ", key1: " << row << ", key2: " << col << ", val: " << value << std::endl;
  };
  my_maptrix.for_all(ijk_lambda);
  /* At a barrier point, are we expecting all
    * asynchronously launched fns to have finished 
    * processing? */
  world.barrier(); 

  #ifdef dbg_impls
  auto visit_lambda = [](auto key1, auto key2, auto value, 
                          int val1,
                          const std::string val2) {
                          //float val2) {
    std::cout << "[ASYNC VISIT]:: Key1: " << key1 << ", Key2: " << key2
              << ", Value: " << value << ", Val1: " << val1 
              << ", Val2: " << val2
              << std::endl;
  };

  //if (world.rank()==0)
  my_maptrix.async_visit_if_exists("row1", "row1002", visit_lambda, world.rank(), std::string("abc"));

  auto visit_col_lambda = [](auto key1, auto key2, auto value, int val1) {
    std::cout << "[COL VISIT]:: Key1: " << key1 << ", Key2: " << key2
              << ", Value: " << value << ", Val1: " << val1 
              << std::endl;
  };
  if (!world.rank() % 2)
    my_maptrix.async_visit_col_const("row1", visit_col_lambda, 1000);
  else
    my_maptrix.async_visit_col_const("row2", visit_col_lambda, 2000);
  world.barrier();
  #endif

  auto visit_col_lambda_mutate = [](auto pmaptrix, int from, auto row, auto col, auto value, int val1) {
    std::cout << "[COL MUTATE VISIT]:: Key1: " << row << ", Key2: " << col
              << ", Value: " << value << ", Val1: " << val1 
              << ", From: " << from << std::endl;
    //pmaptrix->async_insert(row, col, "test");
  };
  if (!world.rank()) {
    //my_maptrix.async_visit_col_mutate("row1001", visit_col_lambda_mutate, 3000);
    my_maptrix.async_visit_or_insert("row1001", "row1", "TEST");
  }
  world.barrier();

  my_maptrix.for_all(ijk_lambda);
  /* At a barrier point, are we expecting all
    * asynchronously launched fns to have finished 
    * processing? */
  world.barrier();

  return 0;
}
