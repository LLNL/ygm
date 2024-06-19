// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once

namespace ygm::container::detail {

template <typename derived_type, typename value_type>

struct base_async_insert_value {
  void async_insert(const value_type& value) {
    derived_type* derived_this = static_cast<derived_type*>(this);

    int dest = derived_this->partitioner.owner(value);

    auto inserter = [](auto pcont, const value_type& item) {
      pcont->local_insert(item);
    };

    derived_this->comm().async(dest, inserter, derived_this->get_ygm_ptr(),
                               value);
  }
};

}  // namespace ygm::container::detail