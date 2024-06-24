// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once

#include <tuple>

namespace ygm::container::detail {

template <typename derived_type, typename for_all_args>
struct base_async_insert {
  void async_insert(const typename std::tuple_element<0, for_all_args>::type& value)
    requires(std::tuple_size_v<for_all_args> == 1)
  {
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

}  // namespace ygm::container::detail