// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once

#include <tuple>
#include <utility>

#define YGM_CHECK_ASYNC_LAMBDA_COMPLIANCE(func, location) \
  static_assert(                                          \
      std::is_trivially_copyable<func>::value &&          \
          std::is_standard_layout<func>::value,           \
      #location                                           \
      " function object must be is_trivially_copyable & is_standard_layout.")
