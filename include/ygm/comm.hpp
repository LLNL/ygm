// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once

#include <deque>
#include <functional>
#include <memory>
#include <utility>
#include <vector>

#include <ygm/detail/byte_vector.hpp>
#include <ygm/detail/comm_environment.hpp>
#include <ygm/detail/comm_router.hpp>
#include <ygm/detail/comm_stats.hpp>
#include <ygm/detail/lambda_map.hpp>
#include <ygm/detail/layout.hpp>
#include <ygm/detail/meta/functional.hpp>
#include <ygm/detail/mpi.hpp>
#include <ygm/detail/ygm_cereal_archive.hpp>
#include <ygm/detail/ygm_ptr.hpp>

namespace ygm {

namespace detail {
class interrupt_mask;
class comm_stats;
class layout;
class comm_router;
}  // namespace detail

class comm {
 private:
  class mpi_irecv_request;
  class mpi_isend_request;
  class header_t;
  friend class detail::interrupt_mask;
  friend class detail::comm_stats;

 public:
  comm(int *argc, char ***argv);

  // TODO:  Add way to detect if this MPI_Comm is already open. E.g., static
  // map<MPI_Comm, impl*>
  comm(MPI_Comm comm);

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
  void async(int dest, AsyncFunction fn, const SendArgs &...args) const {
    const_cast<comm *>(this)->async(dest, fn, args...);
  }

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
  void cf_barrier() const;

  /**
   * @brief Full communicator barrier
   *
   */
  void barrier();

  void barrier() const { const_cast<comm *>(this)->barrier(); }

  void local_progress();

  bool local_process_incoming();

  template <typename Function>
  void local_wait_until(Function fn);

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
  inline T all_reduce(const T &t, MergeFunction merge) const;

  //
  //  Communicator information
  //
  int size() const;
  int rank() const;

  MPI_Comm get_mpi_comm() const;

  const detail::layout &layout() const;

  const detail::comm_router &router() const;

  bool rank0() const { return rank() == 0; }

  template <typename T>
  void mpi_send(const T &data, int dest, int tag, MPI_Comm comm) const;

  template <typename T>
  void mpi_send(const T &data, int dest, int tag) const {
    mpi_send(data, dest, tag, m_comm_other);
  }

  template <typename T>
  T mpi_recv(int source, int tag, MPI_Comm comm) const;

  template <typename T>
  T mpi_recv(int source, int tag) const {
    return mpi_recv<T>(source, tag, m_comm_other);
  }

  template <typename T>
  T mpi_bcast(const T &to_bcast, int root, MPI_Comm comm) const;

  template <typename T>
  T mpi_bcast(const T &to_bcast, int root) const {
    return mpi_bcast(to_bcast, root, m_comm_other);
  }

  std::ostream &cout0() const;
  std::ostream &cerr0() const;
  std::ostream &cout() const;
  std::ostream &cerr() const;

  template <typename... Args>
  void cout(Args &&...args) const;

  template <typename... Args>
  void cerr(Args &&...args) const;

  template <typename... Args>
  void cout0(Args &&...args) const;

  template <typename... Args>
  void cerr0(Args &&...args) const;

  // Private member functions
 private:
  void comm_setup(MPI_Comm comm);

  size_t pack_header(ygm::detail::byte_vector &packed, const int dest,
                     size_t size);

  std::pair<uint64_t, uint64_t> barrier_reduce_counts();

  void flush_next_send(std::deque<int> &dest_queue);
  
  void flush_send_buffer(int dest);

  void handle_completed_send(mpi_isend_request &req_buffer);

  void check_completed_sends();

  void check_if_production_halt_required();

  void flush_all_local_and_process_incoming();

  void flush_to_capacity();

  void post_new_irecv(std::shared_ptr<ygm::detail::byte_vector> &recv_buffer);

  template <typename Lambda, typename... PackArgs>
  size_t pack_lambda(ygm::detail::byte_vector &packed, Lambda l,
                     const PackArgs &...args);

  template <typename Lambda, typename... PackArgs>
  void pack_lambda_broadcast(Lambda l, const PackArgs &...args);

  template <typename Lambda, typename RemoteLogicLambda, typename... PackArgs>
  size_t pack_lambda_generic(ygm::detail::byte_vector &packed, Lambda l,
                             RemoteLogicLambda rll, const PackArgs &...args);

  void queue_message_bytes(const ygm::detail::byte_vector             &packed,
                           const int                     dest);

  void handle_next_receive(std::shared_ptr<ygm::detail::byte_vector> &buffer,
                           const size_t                 buffer_size);

  bool process_receive_queue();

  template <typename... Args>
  std::string outstr(Args &&...args) const;

  template <typename... Args>
  std::string outstr0(Args &&...args) const;

  comm() = delete;

  comm(const comm &c) = delete;

  // Private member variables
 private:
  std::shared_ptr<detail::mpi_init_finalize> pimpl_if;

  MPI_Comm m_comm_async;
  MPI_Comm m_comm_barrier;
  MPI_Comm m_comm_other;

  std::vector<ygm::detail::byte_vector> m_vec_send_buffers;

  size_t                              m_send_local_buffer_bytes = 0;
  std::deque<int>                     m_send_local_dest_queue;
  size_t                              m_send_remote_buffer_bytes = 0;
  std::deque<int>                     m_send_remote_dest_queue;

  std::deque<mpi_irecv_request>                        m_recv_queue;
  std::deque<mpi_isend_request>                        m_send_queue;
  std::vector<std::shared_ptr<ygm::detail::byte_vector>> m_free_send_buffers;

  size_t m_pending_isend_bytes = 0;

  std::deque<std::function<void()>> m_pre_barrier_callbacks;

  bool m_enable_interrupts = true;

  uint64_t m_recv_count = 0;
  uint64_t m_send_count = 0;

  bool m_in_process_receive_queue = false;

  detail::comm_stats             stats;
  const detail::layout           m_layout;
  const detail::comm_environment config = detail::comm_environment(m_layout);
  detail::comm_router            m_router;

  detail::lambda_map<void (*)(comm *, cereal::YGMInputArchive *), uint16_t>
      m_lambda_map;
};

}  // end namespace ygm

#include <ygm/detail/comm.ipp>
