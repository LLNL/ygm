// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once

#include <vector>

namespace ygm {

template <typename T>
class ygm_ptr {
 public:
  ygm_ptr(){};

  T *operator->() { return sptrs[idx]; }

  T &operator*() { return *sptrs[idx]; }

  ygm_ptr(T *t) {
    // TODO:  Should probably have a barrier in here.  Or, in a wrapper
    // function.
    // If ranks create ygm_ptr in different order, big problem.
    idx = sptrs.size();
    sptrs.push_back(t);
  }

  T *get_raw_pointer() { return operator->(); }

  uint32_t index() { return idx; }

  template <class Archive>
  void serialize(Archive &archive) {
    archive(idx);
  }

 private:
  uint32_t idx;
  static std::vector<T *> sptrs;
};

template <typename T>
ygm_ptr<T> make_ygm_pointer(T &t) {
  return ygm_ptr(&t);
}

template <typename T>
std::vector<T *> ygm_ptr<T>::sptrs;

}  // end namespace ygm