// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#include <ygm/comm.hpp>
#include <ygm/container/map.hpp>
#include <ygm/container/maptrix.hpp>
#include <ygm/container/assoc_vector.hpp>

#include <fstream>
#include <iostream>
#include <iomanip>

namespace ygm::algorithms::detail {

template <typename Key, typename Value>
class spmv_impl {
 public:
  using key_type   = Key;
  using value_type = Value;
  using self_type  = spmv_impl<Key, Value>;

  using map_type = ygm::container::assoc_vector<std::string, double>;
  using maptrix_type = ygm::container::maptrix<std::string, double>;

  // This is just for debugging..
  using gt_type = ygm::container::map<std::string, double>;

  spmv_impl(ygm::comm &comm) : m_comm(comm), pthis(this), m_default_value{} {
    m_comm.barrier();
  }

  map_type spmv_op(maptrix_type& my_maptrix, map_type& my_map) {
    
    map_type map_res(m_comm);
    auto map_res_ptr = map_res.get_ygm_ptr();   

    auto kv_lambda = [&my_maptrix, map_res_ptr](auto kv_pair) {
      auto &mptrx_comm = my_maptrix.comm();
      int rank         = mptrx_comm.rank();

      auto col        = kv_pair.first;
      auto col_value  = kv_pair.second;

      auto maptrix_visit_lambda = [](auto col, auto row, auto mat_value, auto vec_value, auto map_res_ptr) {
        auto element_wise = mat_value*vec_value;

        auto append_lambda = [](auto row_id_val, auto update_val) {
          auto row_id = row_id_val->first;
          auto value =  row_id_val->second;
          auto append_val = value+update_val;
          row_id_val->second = row_id_val->second+update_val;
        };

        map_res_ptr->async_visit_or_insert(row, element_wise, append_lambda, element_wise);
      };

      my_maptrix.async_visit_col_const(col, maptrix_visit_lambda, col_value, map_res_ptr);
    };

    my_map.for_all(kv_lambda);
    m_comm.barrier();

    return map_res;
  }

  ~spmv_impl() { m_comm.barrier(); }

 protected:
  spmv_impl() = delete;

  // Do we need to add this as a part of member variable? 
  //map_type      my_map;
  //map_type      map_res;
  //maptrix_type  my_maptrix;
  //gt_type       map_gt;

  value_type                          m_default_value;
  ygm::comm                           m_comm;
  typename ygm::ygm_ptr<self_type>    pthis;
};  
} // namespace ygm::algorithms::detail

#ifdef old_spmv_def
int main(int argc, char **argv) {

  ygm::comm world(&argc, &argv);

  using gt_type = ygm::container::map<std::string, double>;
  using map_type = ygm::container::assoc_vector<std::string, double>;
  using maptrix_type = ygm::container::maptrix<std::string, double>;

  map_type my_map(world);
  map_type map_res(world);
  maptrix_type my_maptrix(world);
  auto my_map_ptr     = my_map.get_ygm_ptr();
  auto map_res_ptr    = map_res.get_ygm_ptr();
  auto my_maptrix_ptr = my_maptrix.get_ygm_ptr();
  
  // This is content from Tim Davis' GraphBlas datasets. 
  //std::ifstream matfile("/g/g90/tom7/codebase/intern_2021/GraphBLAS/Demo/Matrix/ibm32a");
  //std::ifstream matfile("/g/g90/tom7/codebase/intern_2021/GraphBLAS/Demo/Matrix/bcsstk16_1");
  std::ifstream matfile("/g/g90/tom7/codebase/intern_2021/GraphBLAS/Demo/Matrix/bcsstk16");

  //std::ifstream vecfile("/g/g90/tom7/codebase/data/vectors/map_sample_floats__1.txt");
  //std::ifstream vecfile("/g/g90/tom7/codebase/data/vectors/map_sample_floats__2.txt");
  std::ifstream vecfile("/g/g90/tom7/codebase/data/vectors/map_sample_floats__4883.txt");

  std::string key1, key2; 
  double value; 
  if (world.rank0()) {
    while (matfile >> key1 >> key2 >> value) {
      my_maptrix.async_insert(key1, key2, value);
      //std::cout << key1 << " " << key2 << " " << value << std::endl;
    }
    while (vecfile >> key1 >> value) {
      my_map.async_insert(key1, value);
      //if (key1 == "77") {
        //std::cout << key1 << " " << value << std::endl;
      //}
    }
  }
  
  #ifdef dbg
  auto ijk_lambda = [&my_maptrix](auto row, auto col, auto value) {
    auto &mptrx_comm = my_maptrix.comm();
    int rank         = mptrx_comm.rank();
    std::cout << "[MPTRX]: In rank: " << rank << ", key1: " << row << ", key2: " << col << ", val: " << value << std::endl;
  };
  my_maptrix.for_all(ijk_lambda);
  world.barrier();

  auto print_kv_lambda = [](auto res_kv_pair) {
    std::cout << "[In map lambda] key: " << res_kv_pair.first << ", col: " << res_kv_pair.second << std::endl;  
  };
  my_map_ptr->for_all(print_kv_lambda);
  world.barrier();
  #endif

  auto kv_lambda = [&my_maptrix, map_res_ptr](auto kv_pair) {
    auto &mptrx_comm = my_maptrix.comm();
    int rank         = mptrx_comm.rank();

    auto col        = kv_pair.first;
    auto col_value  = kv_pair.second;

    auto maptrix_visit_lambda = [](auto col, auto row, auto mat_value, auto vec_value, auto map_res_ptr) {
      auto element_wise = mat_value*vec_value;

      auto append_lambda = [](auto row_id_val, auto update_val) {
        auto row_id = row_id_val->first;
        auto value =  row_id_val->second;
        auto append_val = value+update_val;
        row_id_val->second = row_id_val->second+update_val;
      };

      map_res_ptr->async_visit_or_insert(row, element_wise, append_lambda, element_wise);
    };

    my_maptrix.async_visit_col_const(col, maptrix_visit_lambda, col_value, map_res_ptr);
  };

  my_map.for_all(kv_lambda);
  world.barrier();

  #ifdef dbg
  gt_type map_gt(world);
  //std::ifstream gtfile("/g/g90/tom7/codebase/data/vectors/spmv_res_32.txt");
  std::ifstream gtfile("/g/g90/tom7/codebase/data/vectors/spmv_res_4883.txt");
  //std::ifstream gtfile("/g/g90/tom7/codebase/data/vectors/spmv_res_4884__1.txt");
  if (world.rank0()) {
    while (gtfile >> key1 >> value) {
      map_gt.async_insert(key1, value);
      //std::cout << key1 << " " << value << std::endl;
    } 
  }
  auto gt_ptr   = map_gt.get_ygm_ptr();

  std::cout << std::fixed;
  std::cout << std::setprecision(8);

  auto check_res_lambda = [gt_ptr](auto res_kv_pair) {
    auto res_key = res_kv_pair.first; 
    auto res_val = res_kv_pair.second; 

    //std::cout << "[In map res lambda] key: " << res_kv_pair.first << ", col: " << res_kv_pair.second << std::endl;
    auto visit_ground = [](auto gt_pair, auto res_val) {

      auto gt_key = gt_pair.first; 
      auto gt_val = gt_pair.second;

      //if (gt_key == "77") {
        if (gt_val != res_val) 
          std::cout << "These values are not equal! " << gt_key << " " << gt_val << " " << res_val << std::endl; 
      //}
      //else 
          //std::cout << "These valuse are equal!" << gt_val << " " << res_val << std::endl; 
    };

    gt_ptr->async_visit(res_key, visit_ground, res_val);
  };
  map_res.for_all(check_res_lambda);
  #endif

  return 0;
}
#endif
