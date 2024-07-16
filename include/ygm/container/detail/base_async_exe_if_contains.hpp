// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once

#include <tuple>
#include <utility>

namespace ygm::container::detail {

template <typename derived_type, typename for_all_args>
struct base_async_exe_if_contains {

  template<typename Function, typename... FuncArgs>
  void async_exe_if_contains(const std::tuple_element<0, for_all_args>::type& value,
                             Function fn, const FuncArgs&... args) {

    derived_type* derived_this = static_cast<derived_type*>(this);

    int dest = derived_this->partitioner.owner(value);

    auto flambda = [](auto pcont,
                      const std::tuple_element<0, for_all_args>::type& value,
                      const FuncArgs&... args) {

      Function* fn = nullptr;
      const bool contains = static_cast<bool>(pcont->local_count(value));
      ygm::meta::apply_optional(*fn, std::make_tuple(pcont),
                                std::forward_as_tuple(value, contains, args...));
    };

    // if constexpr (std::is_invocable<decltype(fn), typename const std::tuple_element<0, for_all_args>::type &,
    //                                 const bool &, FuncArgs &...>() ||
    //               std::is_invocable<decltype(fn), derived_type*, typename const std::tuple_element<0, for_all_args>::type&,
    //                                 FuncArgs &...>()) {

    //   derived_this->comm().async(dest, flambda, derived_this->get_ygm_ptr(),
    //                             value, args...);
    // } else {
    //   static_assert(ygm::detail::always_false<>,
    //                 "remote lambda signature must be invocable with (const "
    //                 "&value_type, const &bool, ...) or (ptr_container_type, const "
    //                 "&value_type, const &bool, ...) signatures");
    // } 
    derived_this->comm().async(dest, flambda, derived_this->get_ygm_ptr(),
                              value, args...);
  }
};

}  // namespace ygm::container::detail