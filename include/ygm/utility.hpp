// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once

#include <array>
#include <mutex>
#include <ygm/detail/mpi.hpp>

namespace ygm {
class timer {
public:
  timer() { reset(); }

  double elapsed() { return MPI_Wtime() - m_start; }

  void reset() { m_start = MPI_Wtime(); }

private:
  double m_start;
};

// Add padding to avoid false sharing from the act of grabbing a lock. Cache
// lines should be 64 bytes, but I need 128 here to prevent false sharing
// (probably result of int division)
constexpr size_t mutex_padding = 128 / sizeof(std::mutex);

struct DefaultLockBankTag {};

template <int NumBanks, typename Tag = DefaultLockBankTag> class lock_bank {
public:
  lock_bank() {}

  static const int num_banks() { return NumBanks; }

  static std::unique_lock<std::mutex> mutex_lock(const size_t index) {
    return std::unique_lock<std::mutex>(
        lock_bank<NumBanks, Tag>::arr_mutex[(index % NumBanks) *
                                            mutex_padding]);
  }

private:
  inline static std::array<std::mutex, NumBanks * mutex_padding> arr_mutex;
};

} // namespace ygm
