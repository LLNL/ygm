// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once

#include <tuple>
#include <utility>
#include <ygm/container/detail/base_concepts.hpp>

namespace ygm::container::detail {

template <typename derived_type, typename for_all_args>
struct base_batch_erase_key {
  template <typename Container>
  void erase(const Container &cont) requires detail::HasForAll<Container> &&
      detail::SingleItemTuple<typename Container::for_all_args> &&
      detail::AtLeastOneItemTuple<for_all_args> && std::convertible_to<
          std::tuple_element_t<0, typename Container::for_all_args>,
          std::tuple_element_t<0, for_all_args>> {
    derived_type *derived_this = static_cast<derived_type *>(this);

    cont.for_all(
        [derived_this](const auto &key) { derived_this->async_erase(key); });

    derived_this->comm().barrier();
  }
};
}  // namespace ygm::container::detail
