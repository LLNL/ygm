// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once

#include <type_traits>
#include <ygm/container/array.hpp>
#include <ygm/container/detail/reducing_adapter.hpp>
#include <ygm/container/map.hpp>
#include <ygm/detail/ygm_traits.hpp>

namespace ygm::container {

/**
 * @brief  Collective reduce_by_key that outputs a ygm::map<Key,Value>
 *
 * @tparam Key
 * @tparam Value
 * @tparam Container
 * @tparam ReductionFunction
 * @param container
 * @param reducer
 * @param cm
 * @return ygm::map<Key, Value>
 */
template <typename Key, typename Value, typename Container,
          typename ReductionFunction>
ygm::container::map<Key, Value> reduce_by_key_map(Container&        container,
                                                  ReductionFunction reducer,
                                                  comm              cm) {
  cm.barrier();
  ygm::container::map<Key, Value> to_return(cm);

  auto the_reducer =
      ygm::container::detail::make_reducing_adapter(to_return, reducer);

  auto lambda_two = [&the_reducer](const Key& k, const Value& v) {
    the_reducer.async_reduce(k, v);
  };

  auto lambda_pair = [&the_reducer](const std::pair<const Key, Value>& kv) {
    the_reducer.async_reduce(kv.first, kv.second);
  };

  if constexpr (ygm::detail::is_for_each_invocable<
                    Container, decltype(lambda_two)>::value) {
    std::for_each(container.begin(), container.end(), lambda_two);
  } else if constexpr (ygm::detail::is_for_each_invocable<
                           Container, decltype(lambda_pair)>::value) {
    std::for_each(container.begin(), container.end(), lambda_pair);
  } else if constexpr (ygm::detail::is_for_all_invocable<
                           Container, decltype(lambda_two)>::value) {
    container.for_all(lambda_two);
  } else if constexpr (ygm::detail::is_for_all_invocable<
                           Container, decltype(lambda_pair)>::value) {
    container.for_all(lambda_pair);
  } else {
    static_assert(ygm::detail::always_false<>,
                  "Unsupported Lambda or Container");
  }

  cm.barrier();
  return to_return;
}
}  // namespace ygm::container