// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once

#include <ygm/collective.hpp>
#include <ygm/comm.hpp>

namespace ygm {
namespace detail {
bool distributed_string_enumerators_agree(const string_enumerator &str_enum,
                                          ygm::comm               &comm) {
  return ygm::is_same(str_enum, comm);
}
}  // namespace detail
}  // namespace ygm
