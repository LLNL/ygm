// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once

#include <tuple>
#include <utility>

namespace ygm::container::detail {

template <typename derived_type, typename for_all_args>
<<<<<<< HEAD
struct base_async_insert {
  void async_insert(const typename std::tuple_element<0, for_all_args>::type& value)
    requires(std::tuple_size_v<for_all_args> == 1)
  {
=======
struct base_async_insert_value {
  void async_insert(const std::tuple_element<0, for_all_args>::type& value) {
>>>>>>> b4ca7d43cd3ad6e8fcacf4d1cda0fdcb074ff7d8
    derived_type* derived_this = static_cast<derived_type*>(this);

    int dest = derived_this->partitioner.owner(value);

    derived_this->comm().cout("Dest: ", dest);   

    auto inserter = [](auto                                             pcont,
                       const typename std::tuple_element<0, for_all_args>::type& item) { 
      pcont->local_insert(item);
    };

    derived_this->comm().async(dest, inserter, derived_this->get_ygm_ptr(),
                               value);
  }
};

template <typename derived_type, typename for_all_args>
struct base_async_insert_key_value {
  void async_insert(const std::tuple_element<0, for_all_args>::type& key,
                    const std::tuple_element<1, for_all_args>::type& value)
    requires requires(for_all_args f) { std::tuple_size_v<for_all_args> == 2; }
  {
    derived_type* derived_this = static_cast<derived_type*>(this);

    int dest = derived_this->partitioner.owner(key);

    auto inserter = [](auto                                             pcont,
                       const std::tuple_element<0, for_all_args>::type& key,
                       const std::tuple_element<1, for_all_args>::type& value) {
      pcont->local_insert(key, value);
    };

    derived_this->comm().async(dest, inserter, derived_this->get_ygm_ptr(), key,
                               value);
  }

  void async_insert(
      const std::pair<typename std::tuple_element<0, for_all_args>::type,
                      typename std::tuple_element<1, for_all_args>::type>& kvp)
    requires requires(for_all_args f) { std::tuple_size_v<for_all_args> == 2; }
  {
    async_insert(kvp.first, kvp.second);
  }
};

}  // namespace ygm::container::detail