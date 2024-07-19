// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once

#include <tuple>

namespace ygm::container::detail {

template <typename T>
concept SingleItemTuple = requires(T v) {
  requires std::tuple_size<T>::value == 1;
};

template <typename T>
concept DoubleItemTuple = requires(T v) {
  requires std::tuple_size<T>::value == 2;
};

template <typename T>
concept AtLeastOneItemTuple = requires(T v) {
  requires std::tuple_size<T>::value >= 1;
};

}