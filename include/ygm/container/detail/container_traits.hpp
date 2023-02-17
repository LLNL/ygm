// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once
#include <type_traits>
#include <ygm/container/array.hpp>
#include <ygm/container/bag.hpp>
#include <ygm/container/counting_set.hpp>
#include <ygm/container/disjoint_set.hpp>
#include <ygm/container/map.hpp>
#include <ygm/container/set.hpp>

namespace ygm::container::detail {

// is_map
template <typename T>
struct is_map_impl : std::false_type {};

template <typename... Ts>
struct is_map_impl<map<Ts...>> : std::true_type {};

template <typename T>
constexpr bool is_map = is_map_impl<T>::value;

// is_array
template <typename T>
struct is_array_impl : std::false_type {};

template <typename... Ts>
struct is_array_impl<array<Ts...>> : std::true_type {};

template <typename T>
constexpr bool is_array = is_array_impl<T>::value;

// is_bag
template <typename T>
struct is_bag_impl : std::false_type {};

template <typename... Ts>
struct is_bag_impl<bag<Ts...>> : std::true_type {};

template <typename T>
constexpr bool is_bag = is_bag_impl<T>::value;

// is_set
template <typename T>
struct is_set_impl : std::false_type {};

template <typename... Ts>
struct is_set_impl<set<Ts...>> : std::true_type {};

template <typename T>
constexpr bool is_set = is_set_impl<T>::value;

// is_disjoint_set
template <typename T>
struct is_disjoint_set_impl : std::false_type {};

template <typename... Ts>
struct is_disjoint_set_impl<disjoint_set<Ts...>> : std::true_type {};

template <typename T>
constexpr bool is_disjoint_set = is_disjoint_set_impl<T>::value;

// is_counting_set
template <typename T>
struct is_counting_set_impl : std::false_type {};

template <typename... Ts>
struct is_counting_set_impl<counting_set<Ts...>> : std::true_type {};

template <typename T>
constexpr bool is_counting_set = is_counting_set_impl<T>::value;

}  // namespace ygm::container::detail
