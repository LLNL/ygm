// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once

#include <functional>
#include <tuple>
#include <type_traits>

namespace ygm {

namespace meta {

template <class...>
constexpr std::false_type always_false{};

template <typename>
struct is_std_pair : std::false_type {};

template <typename T1, typename T2>
struct is_std_pair<std::pair<T1, T2>> : std::true_type {};

}  // namespace meta

}  // namespace ygm
