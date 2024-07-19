// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once

#include <tuple>
#include <utility>
#include <ygm/container/detail/base_concepts.hpp>
#include <ygm/detail/interrupt_mask.hpp>

namespace ygm::container::detail {

template <typename derived_type, typename for_all_args>
struct base_async_visit {
  template <typename Visitor, typename... VisitorArgs>
  void async_visit(const std::tuple_element<0, for_all_args>::type& key,
                   Visitor visitor, const VisitorArgs&... args)
    requires DoubleItemTuple<for_all_args>
  {
    derived_type* derived_this = static_cast<derived_type*>(this);

    int dest = derived_this->partitioner.owner(key);

    auto vlambda = [](auto                                             pcont,
                      const std::tuple_element<0, for_all_args>::type& key,
                      const VisitorArgs&... args) {
      Visitor* vis = nullptr;
      pcont->local_visit(key, *vis, args...);
    };

    derived_this->comm().async(dest, vlambda, derived_this->get_ygm_ptr(), key,
                               args...);
  }

  template <typename Visitor, typename... VisitorArgs>
  void async_visit_if_contains(
      const std::tuple_element<0, for_all_args>::type& key, Visitor visitor,
      const VisitorArgs&... args)
    requires DoubleItemTuple<for_all_args>
  {
    derived_type* derived_this = static_cast<derived_type*>(this);

    int dest = derived_this->partitioner.owner(key);

    auto vlambda = [](auto                                             pcont,
                      const std::tuple_element<0, for_all_args>::type& key,
                      const VisitorArgs&... args) {
      Visitor* vis = nullptr;
      pcont->local_visit_if_contains(key, *vis, args...);
    };

    derived_this->comm().async(dest, vlambda, derived_this->get_ygm_ptr(), key,
                               args...);
  }

  template <typename Visitor, typename... VisitorArgs>
  void async_visit_if_contains(
      const std::tuple_element<0, for_all_args>::type& key, Visitor visitor,
      const VisitorArgs&... args) const
    requires DoubleItemTuple<for_all_args>
  {
    const derived_type* derived_this = static_cast<const derived_type*>(this);

    int dest = derived_this->partitioner.owner(key);

    auto vlambda = [](const auto                                       pcont,
                      const std::tuple_element<0, for_all_args>::type& key,
                      const VisitorArgs&... args) {
      Visitor* vis = nullptr;
      pcont->local_visit_if_contains(key, *vis, args...);
    };

    derived_this->comm().async(dest, vlambda, derived_this->get_ygm_ptr(), key,
                               args...);
  }

  // todo:   async_insert_visit()
};

}  // namespace ygm::container::detail