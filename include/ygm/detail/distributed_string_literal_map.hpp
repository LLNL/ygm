// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once

//#include <ygm/collective.hpp>
#include <ygm/comm.hpp>
#include <ygm/detail/distributed_string_enumeration.hpp>
#include <ygm/detail/string_literal_map.hpp>

namespace ygm {
namespace detail {
template <typename T>
void string_literal_map_match_keys(string_literal_map<T> &str_map,
                                   ygm::comm             &comm) {
  ASSERT_RELEASE(
      distributed_string_enumerators_agree(str_map.m_enumerator, comm));

  comm.barrier();
  std::vector<int> local_mask;
  for (size_t i = 0; i < str_map.capacity(); ++i) {
    if (str_map.is_filled(i)) {
      local_mask.push_back(true);
    } else {
      local_mask.push_back(false);
    }
  }
  ASSERT_MPI(MPI_Allreduce(local_mask.data(), MPI_IN_PLACE, local_mask.size(),
                           detail::mpi_typeof(local_mask[0]), MPI_LOR,
                           comm.get_mpi_comm()));

  // Adds default entry if none is found locally but another rank has an entry
  for (size_t i = 0; i < local_mask.size(); ++i) {
    if (local_mask[i]) {
      str_map.get_value_from_index(i);
    }
  }
}
}  // namespace detail
}  // namespace ygm
