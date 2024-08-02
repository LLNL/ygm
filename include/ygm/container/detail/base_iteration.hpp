// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once

#include <tuple>
#include <ygm/collective.hpp>
#include <ygm/container/detail/base_concepts.hpp>

namespace ygm::container::detail {

template <typename derived_type, typename FilterFunction>
class filter_proxy;

template <typename derived_type, typename MapFunction>
class map_proxy;

template <typename derived_type>
class flatten_proxy;

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
    YGM_ASSERT_RELEASE(derived_this->local_size() >
                   0);  // empty partition not handled yet

    using value_type = typename std::tuple_element<0, for_all_args>::type;
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

  template <typename YGMContainer>
  void collect(YGMContainer& c) const {
    const derived_type* derived_this = static_cast<const derived_type*>(this);
    auto clambda = [&c](const std::tuple_element<0, for_all_args>::type& item) {
      c.async_insert(item);
    };
    derived_this->for_all(clambda);
  }

  template <typename MapType, typename ReductionOp>
  void reduce_by_key(MapType& map, ReductionOp reducer) const {
    const derived_type* derived_this = static_cast<const derived_type*>(this);
    // static_assert ygm::map
    using reduce_key_type   = typename MapType::key_type;
    using reduce_value_type = typename MapType::mapped_type;
    if constexpr (std::tuple_size<for_all_args>::value == 1) {
      // must be a std::pair
      auto rbklambda = [&map, reducer](std::pair<reduce_key_type, reduce_value_type> kvp) {
        map.async_reduce(kvp.first, kvp.second, reducer);
      };
      derived_this->for_all(rbklambda);
    } else {
      static_assert(std::tuple_size<for_all_args>::value == 2);
      auto rbklambda = [&map, reducer](const reduce_key_type& key, const reduce_value_type& value) {
        map.async_reduce(key, value, reducer);
      };
      derived_this->for_all(rbklambda);
    }
  }

  template <typename MapFunction>
  map_proxy<derived_type, MapFunction> map(MapFunction ffn);

  flatten_proxy<derived_type> flatten();

  template <typename FilterFunction>
  filter_proxy<derived_type, FilterFunction> filter(FilterFunction ffn);

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
};

}  // namespace ygm::container::detail

#include <ygm/container/detail/filter_proxy.hpp>
#include <ygm/container/detail/flatten_proxy.hpp>
#include <ygm/container/detail/map_proxy.hpp>

namespace ygm::container::detail {

template <typename derived_type, typename for_all_args>
template <typename MapFunction>
map_proxy<derived_type, MapFunction>
base_iteration<derived_type, for_all_args>::map(MapFunction ffn) {
  derived_type* derived_this = static_cast<derived_type*>(this);
  return map_proxy<derived_type, MapFunction>(*derived_this, ffn);
}

template <typename derived_type, typename for_all_args>
inline flatten_proxy<derived_type>
base_iteration<derived_type, for_all_args>::flatten() {
  // static_assert(
  //     type_traits::is_vector<std::tuple_element<0, for_all_args>>::value);
  derived_type* derived_this = static_cast<derived_type*>(this);
  return flatten_proxy<derived_type>(*derived_this);
}

template <typename derived_type, typename for_all_args>
template <typename FilterFunction>
filter_proxy<derived_type, FilterFunction>
base_iteration<derived_type, for_all_args>::filter(FilterFunction ffn) {
  derived_type* derived_this = static_cast<derived_type*>(this);
  return filter_proxy<derived_type, FilterFunction>(*derived_this, ffn);
}

}  // namespace ygm::container::detail