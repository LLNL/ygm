// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once

#include <functional>
#include <memory>
#include <vector>
#include <ygm/detail/comm_router.hpp>
#include <ygm/detail/layout.hpp>
#include <ygm/detail/mpi.hpp>
#include <ygm/detail/ygm_ptr.hpp>

namespace ygm {

namespace detail {
class interrupt_mask;
class comm_stats;
}  // namespace detail

class comm {
 private:
  class impl;
  friend class detail::interrupt_mask;
  friend class detail::comm_stats;

 public:
  comm(int *argc, char ***argv);

  // TODO:  Add way to detect if this MPI_Comm is already open. E.g., static
  // map<MPI_Comm, impl*>
  comm(MPI_Comm comm);

  // Constructor to allow comm::impl to build temporary comm using itself as the
  // impl
  comm(std::shared_ptr<impl> impl_ptr);

  ~comm();

  /**
   * @brief Prints a welcome message with configuration details.
   *
   */
  void welcome(std::ostream &os = std::cout);

  void stats_reset();
  void stats_print(const std::string &name = "", std::ostream &os = std::cout);

  //
  //  Asynchronous rpc interfaces.   Can be called inside OpenMP loop
  //

  template <typename AsyncFunction, typename... SendArgs>
  void async(int dest, AsyncFunction fn, const SendArgs &...args);

  template <typename AsyncFunction, typename... SendArgs>
  void async_bcast(AsyncFunction fn, const SendArgs &...args);

  template <typename AsyncFunction, typename... SendArgs>
  void async_mcast(const std::vector<int> &dests, AsyncFunction fn,
                   const SendArgs &...args);

  //
  // Collective operations across all ranks.  Cannot be called inside OpenMP
  // region.

  /**
   * @brief Control Flow Barrier
   * Only blocks the control flow until all processes in the communicator have
   * called it. See:  MPI_Barrier()
   */
  void cf_barrier();

  /**
   * @brief Full communicator barrier
   *
   */
  void barrier();

  template <typename T>
  ygm_ptr<T> make_ygm_ptr(T &t);

  /**
   * @brief Registers a callback that will be executed prior to the barrier
   * completion
   *
   * @param fn callback function
   */
  void register_pre_barrier_callback(const std::function<void()> &fn);

  template <typename T>
  T all_reduce_sum(const T &t) const;

  template <typename T>
  T all_reduce_min(const T &t) const;

  template <typename T>
  T all_reduce_max(const T &t) const;

  template <typename T, typename MergeFunction>
  inline T all_reduce(const T &t, MergeFunction merge);

  //
  //  Communicator information
  //
  int size() const;
  int rank() const;

  MPI_Comm get_mpi_comm() const;

  const detail::layout &layout() const;

  const detail::comm_router &router() const;

  std::ostream &cout0() const {
    static std::ostringstream dummy;
    dummy.clear();
    if (rank() == 0) {
      return std::cout;
    }
    return dummy;
  }

  std::ostream &cerr0() const {
    static std::ostringstream dummy;
    dummy.clear();
    if (rank() == 0) {
      return std::cerr;
    }
    return dummy;
  }

  std::ostream &cout() const {
    std::cout << rank() << ": ";
    return std::cout;
  }

  std::ostream &cerr() const {
    std::cerr << rank() << ": ";
    return std::cerr;
  }

  bool rank0() const { return rank() == 0; }

  template <typename... Args>
  void cout(Args &&...args) const {
    std::cout << outstr(args...) << std::endl;
  }

  template <typename... Args>
  void cerr(Args &&...args) const {
    std::cerr << outstr(args...) << std::endl;
  }

  template <typename... Args>
  void cout0(Args &&...args) const {
    if (rank0()) {
      std::cout << outstr0(args...) << std::endl;
    }
  }

  template <typename... Args>
  void cerr0(Args &&...args) const {
    if (rank0()) {
      std::cerr << outstr0(args...) << std::endl;
    }
  }

 private:
  template <typename... Args>
  std::string outstr0(Args &&...args) const {
    std::stringstream ss;
    (ss << ... << args);
    return ss.str();
  }

  template <typename... Args>
  std::string outstr(Args &&...args) const {
    std::stringstream ss;
    (ss << rank() << ": " << ... << args);
    return ss.str();
  }

  comm() = delete;

  std::shared_ptr<impl>                      pimpl;
  std::shared_ptr<detail::mpi_init_finalize> pimpl_if;
};

}  // end namespace ygm

#include <ygm/detail/comm_impl.hpp>
