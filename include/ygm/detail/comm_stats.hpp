// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once

#include <ygm/comm.hpp>

namespace ygm {
namespace detail {
class comm_stats {
 public:
  class timer {
   public:
    timer(double& _timer) : m_timer(_timer), m_start_time(MPI_Wtime()) {}

    ~timer() { m_timer += (MPI_Wtime() - m_start_time); }

   private:
    double& m_timer;
    double  m_start_time;
  };

  comm_stats() : m_time_start(MPI_Wtime()) {}

  void print(const std::string& name, std::ostream& os, ygm::comm& comm) {
    std::stringstream sstr;
    sstr << "============== STATS =================\n"
         << "NAME                     = " << name << "\n"
         << "TIME                     = " << MPI_Wtime() - m_time_start << "\n"
         << "GLOBAL_ASYNC_COUNT       = " << comm.all_reduce_sum(m_async_count)
         << "\n"
         << "GLOBAL_ISEND_COUNT       = " << comm.all_reduce_sum(m_isend_count)
         << "\n"
         << "GLOBAL_ISEND_BYTES       = " << comm.all_reduce_sum(m_isend_bytes)
         << "\n"
         << "MAX_WAITSOME_ISEND_IRECV = "
         << comm.all_reduce_max(m_waitsome_isend_irecv_time) << "\n"
         << "MAX_WAITSOME_IALLREDUCE  = "
         << comm.all_reduce_max(m_waitsome_iallreduce_time) << "\n"
         << "COUNT_IALLREDUCE         = " << m_iallreduce_count << "\n"
         << "======================================";
    if (comm.rank0()) {
      os << sstr.str() << std::endl;
    }
  }

  void isend(int dest, size_t bytes) {
    m_isend_count += 1;
    m_isend_bytes += bytes;
  }

  void irecv(int source, size_t bytes) {
    m_irecv_count += 1;
    m_irecv_bytes += bytes;
  }

  void async(int dest) { m_async_count += 1; }

  void rpc_execute() { m_rpc_count += 1; }

  void routing() { m_route_count += 1; }

  void isend_test() { m_isend_test_count += 1; }

  void irecv_test() { m_irecv_test_count += 1; }

  void iallreduce() { m_iallreduce_count += 1; }

  timer waitsome_isend_irecv() {
    m_waitsome_isend_irecv_count += 1;
    return timer(m_waitsome_isend_irecv_time);
  }

  timer waitsome_iallreduce() {
    m_waitsome_iallreduce_count += 1;
    return timer(m_waitsome_iallreduce_time);
  }

  void reset() {
    m_async_count                = 0;
    m_rpc_count                  = 0;
    m_route_count                = 0;
    m_isend_count                = 0;
    m_isend_bytes                = 0;
    m_isend_test_count           = 0;
    m_irecv_count                = 0;
    m_irecv_bytes                = 0;
    m_irecv_test_count           = 0;
    m_waitsome_isend_irecv_time  = 0.0f;
    m_waitsome_isend_irecv_count = 0.0f;
    m_iallreduce_count           = 0;
    m_waitsome_iallreduce_time   = 0.0f;
    m_waitsome_iallreduce_count  = 0;
    m_time_start                 = MPI_Wtime();
  }

 private:
  size_t m_async_count = 0;
  size_t m_rpc_count   = 0;
  size_t m_route_count = 0;

  size_t m_isend_count      = 0;
  size_t m_isend_bytes      = 0;
  size_t m_isend_test_count = 0;

  size_t m_irecv_count      = 0;
  size_t m_irecv_bytes      = 0;
  size_t m_irecv_test_count = 0;

  double m_waitsome_isend_irecv_time  = 0.0f;
  size_t m_waitsome_isend_irecv_count = 0.0f;

  size_t m_iallreduce_count          = 0;
  double m_waitsome_iallreduce_time  = 0.0f;
  size_t m_waitsome_iallreduce_count = 0;

  double m_time_start = 0.0;
};
}  // namespace detail
}  // namespace ygm