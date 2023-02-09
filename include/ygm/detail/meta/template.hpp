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

}  // namespace meta

}  // namespace ygm
