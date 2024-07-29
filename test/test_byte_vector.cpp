// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#undef NDEBUG
#include <ygm/detail/ygm_cereal_archive.hpp>
#include <ygm/detail/byte_vector.hpp>
#include <fstream>
#include <iostream>
#include <vector>
#include <string>

int main() {
  std::vector<std::string> vec_sentences = {
      "Four score and seven years ago",
      "our fathers brought forth on this continent",
      "a new nation conceived in liberty"};

  ygm::detail::byte_vector buffer;
  {
    for(const auto& s : vec_sentences) {
      buffer.push_bytes(s.data(), s.size());
    }
  }

  {
    std::vector<char> output(buffer.size());
    memcpy((void*)output.data(), (void*)buffer.data(), buffer.size());
    auto str_it = output.begin(;)
    auto bv_it  = buffer.begin();
    for(const auto& s : vec_sentences) {
      for(int i = 0; i < s.size(); i++) {
        ASSERT_RELEASE(s[i] == (char)(*bv_it) && s[i] == (*str_it));
        bv_it++;
        str_it++;
      }
    }
  }

  return 0;
}