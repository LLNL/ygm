// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once

#include <tuple>
#include <ygm/collective.hpp>
#include <ygm/container/detail/base_concepts.hpp>

namespace ygm::container::detail {

template <typename derived_type, typename for_all_args>
struct base_iteration {
  template <typename Function>
  void for_all(Function fn) {
    derived_type* derived_this = static_cast<derived_type*>(this);
    derived_this->comm().barrier();
    derived_this->local_for_all(fn);
  }

  template <typename Function>
  void for_all(Function fn) const {
    const derived_type* derived_this = static_cast<const derived_type*>(this);
    derived_this->comm().barrier();
    derived_this->local_for_all(fn);
  }

  template <typename STLContainer>
  void gather(STLContainer& gto, int rank) const {
    // TODO, make an all gather version that defaults to rank = -1 & uses a temp
    // container.
    bool                 all_gather   = (rank == -1);
    static STLContainer* spgto        = &gto;
    const derived_type*  derived_this = static_cast<const derived_type*>(this);
    const ygm::comm&     mycomm       = derived_this->comm();

    auto glambda = [&mycomm, rank](const auto& value) {
      mycomm.async(
          rank, [](const auto& value) { generic_insert(*spgto, value); },
          value);
    };

    for_all(glambda);

    derived_this->comm().barrier();
  }

  template <typename MergeFunction>
  std::tuple_element<0, for_all_args>::type reduce(MergeFunction merge) const
    requires SingleItemTuple<for_all_args>
  {
    const derived_type* derived_this = static_cast<const derived_type*>(this);
    derived_this->comm().barrier();
    ASSERT_RELEASE(derived_this->local_size() >
                   0);  // empty partition not handled yet

    using value_type = std::tuple_element<0, for_all_args>::type;
    bool first       = true;

    value_type to_return;

    auto rlambda = [&to_return, &first, &merge](const value_type& value) {
      if (first) {
        to_return = value;
        first     = false;
      } else {
        to_return = merge(to_return, value);
      }
    };

    derived_this->for_all(rlambda);

    derived_this->comm().barrier();

    return ::ygm::all_reduce(to_return, merge, derived_this->comm());
  }

 private:
  template <typename STLContainer, typename Value>
    requires requires(STLContainer stc, Value v) { stc.push_back(v); }
  static void generic_insert(STLContainer& stc, const Value& value) {
    stc.push_back(value);
  }

  template <typename STLContainer, typename Value>
    requires requires(STLContainer stc, Value v) { stc.insert(v); }
  static void generic_insert(STLContainer& stc, const Value& value) {
    stc.insert(value);
  }

  // * reduce_by_key   bag<int>.reduce_by_key -- invalid
  //                  bag<pair<string, int>>.reduce_by_key(merge(int, int));
  // * map()
  // * filter()
  // * flatten()
  // unpack()
  // collect()
};

}  // namespace ygm::container::detail