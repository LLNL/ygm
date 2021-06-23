// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once

#include <memory>
#include <ygm/detail/mpi.hpp>

namespace ygm {

class comm {
public:
  comm(int *argc, char ***argv, int buffer_capacity);

  // TODO:  Add way to detect if this MPI_Comm is already open. E.g., static
  // map<MPI_Comm, impl*>
  comm(MPI_Comm comm, int buffer_capacity);

  ~comm();

  //
  //  Asynchronous rpc interfaces.   Can be called inside OpenMP loop
  //

  template <typename AsyncFunction, typename... SendArgs>
  void async(int dest, AsyncFunction fn, const SendArgs &... args);

  template <typename... SendArgs>
  void async_preempt(int dest, const SendArgs &... args);

  template <typename... SendArgs> void async_bcast(const SendArgs &... args);

  template <typename... SendArgs>
  void async_bcast_preempt(const SendArgs &... args);

  void async_flush(int rank);
  void async_flush_bcast();
  void async_flush_all();

  //
  // Collective operations across all ranks.  Cannot be called inside OpenMP
  // region.
  // TODO:  Add guards to check for openmp region.
  //

  void barrier();

  template <typename T> T all_reduce_sum(const T &t) const;

  template <typename T> T all_reduce_min(const T &t) const;

  template <typename T> T all_reduce_max(const T &t) const;

  template <typename T, typename MergeFunction>
  inline T all_reduce(const T &t, MergeFunction merge);

  //
  //  Communicator information
  //
  int size() const;
  int rank() const;
  int local_size() const;
  int local_rank() const;
  int remote_size() const;
  int remote_rank() const;

  //
  //	Counters
  //
  int64_t local_bytes_sent() const;
  int64_t global_bytes_sent() const;
  void reset_bytes_sent_counter();
  int64_t local_rpc_calls() const;
  int64_t global_rpc_calls() const;
  void reset_rpc_call_counter();

  std::ostream &cout0() {
    static std::ostringstream dummy;
    dummy.clear();
    if (rank() == 0) {
      return std::cout;
    }
    return dummy;
  }

  std::ostream &cerr0() {
    static std::ostringstream dummy;
    dummy.clear();
    if (rank() == 0) {
      return std::cerr;
    }
    return dummy;
  }

  std::ostream &cout() {
    std::cout << rank() << ": ";
    return std::cout;
  }

  std::ostream &cerr() {
    std::cerr << rank() << ": ";
    return std::cout;
  }

  bool rank0() const { return rank() == 0; }

  template <typename... Args> void cout(Args &&... args) {
    (cout() << ... << args) << std::endl;
  }

  template <typename... Args> void cerr(Args &&... args) {
    (cerr() << ... << args) << std::endl;
  }

  template <typename... Args> void cout0(Args &&... args) {
    if (rank0()) {
      (std::cout << ... << args) << std::endl;
    }
  }

  template <typename... Args> void cerr0(Args &&... args) {
    if (rank0()) {
      (std::cerr << ... << args) << std::endl;
    }
  }

private:
  comm() = delete;

  class impl;
  std::shared_ptr<impl> pimpl;
  std::shared_ptr<detail::mpi_init_finalize> pimpl_if;
};

} // end namespace ygm

#include <ygm/detail/comm_impl.hpp>
