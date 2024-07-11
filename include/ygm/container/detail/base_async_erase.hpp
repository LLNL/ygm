// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once

#include <tuple>
#include <utility>

namespace ygm::container::detail {

template <typename derived_type, typename for_all_args>
struct base_async_erase {
  void async_erase(const std::tuple_element<0, for_all_args>::type& key)
  // requires requires(for_all_args f) { std::tuple_size_v<for_all_args> =>1;
  // }
  {
    derived_type* derived_this = static_cast<derived_type*>(this);

    int dest = derived_this->partitioner.owner(key);

    auto updater = [](auto                                             pcont,
                      const std::tuple_element<0, for_all_args>::type& key) {
      pcont->local_erase(key);
    };

    derived_this->comm().async(dest, updater, derived_this->get_ygm_ptr(), key);
  }

  // void async_erase(const std::tuple_element<0, for_all_args>::type& key,
  //                  const std::tuple_element<1, for_all_args>::type& value)
  //   requires requires(for_all_args f) { std::tuple_size_v<for_all_args> == 2; }
  // {
  //   derived_type* derived_this = static_cast<derived_type*>(this);

  //   int dest = derived_this->partitioner.owner(key);

  //   auto updater = [](auto                                             pcont,
  //                     const std::tuple_element<0, for_all_args>::type& key,
  //                     const std::tuple_element<1, for_all_args>::type& value) {
  //     pcont->local_erase(key, value);
  //   };

  //   derived_this->comm().async(dest, updater, derived_this->get_ygm_ptr(), key,
  //                              value);
  // }
};

}  // namespace ygm::container::detail