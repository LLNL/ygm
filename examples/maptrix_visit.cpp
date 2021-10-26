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

  auto visit_lambda = [](auto key1, auto key2, auto value, 
                          int val1,
                          //, const std::string val2) {
                          //const std::string val2) {
                          float val2) {
    std::cout << "Key1: " << key1 << ", Key2: " << key2
              << ", Value: " << value << ", Val1: " << val1 
              << ", Val2: " << val2
              << std::endl;
  };
  my_maptrix.async_visit_if_exists("row1", "row2", visit_lambda, 1023, 1.0);

  return 0;
}
