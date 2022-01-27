// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once

#include <atomic>
#include <deque>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include <ygm/detail/layout.hpp>
#include <ygm/detail/meta/functional.hpp>
#include <ygm/detail/mpi.hpp>
#include <ygm/detail/ygm_cereal_archive.hpp>
#include <ygm/detail/ygm_ptr.hpp>

namespace ygm {

class comm::impl : public std::enable_shared_from_this<comm::impl> {
 public:
  impl(MPI_Comm c, int buffer_capacity) : m_layout(c) {
    ASSERT_MPI(MPI_Comm_dup(c, &m_comm_async));
    ASSERT_MPI(MPI_Comm_dup(c, &m_comm_barrier));
    ASSERT_MPI(MPI_Comm_dup(c, &m_comm_other));
    ASSERT_MPI(MPI_Comm_size(m_comm_async, &m_comm_size));
    ASSERT_MPI(MPI_Comm_rank(m_comm_async, &m_comm_rank));
    m_buffer_capacity_bytes = buffer_capacity;

    m_vec_send_buffers.resize(m_comm_size);
    // launch listener thread
    m_listener = std::thread(&impl::listener_thread, this);
  }

  ~impl() {
    // send kill signal to self (listener thread)
    ASSERT_RELEASE(MPI_Send(NULL, 0, MPI_BYTE, m_comm_rank, 0, m_comm_async) ==
                   MPI_SUCCESS);
    // Join listener thread.
    m_listener.join();
    // Free cloned communicator.
    ASSERT_RELEASE(MPI_Barrier(m_comm_async) == MPI_SUCCESS);
    ASSERT_RELEASE(MPI_Comm_free(&m_comm_async) == MPI_SUCCESS);
    ASSERT_RELEASE(MPI_Comm_free(&m_comm_barrier) == MPI_SUCCESS);
    ASSERT_RELEASE(MPI_Comm_free(&m_comm_other) == MPI_SUCCESS);
  }

  int size() const { return m_comm_size; }
  int rank() const { return m_comm_rank; }

  template <typename... SendArgs>
  void async(int dest, const SendArgs &...args) {
    ASSERT_DEBUG(dest < m_comm_size);
    static size_t recursion_detector = 0;
    ++recursion_detector;
    if (dest == m_comm_rank) {
      local_receive(std::forward<const SendArgs>(args)...);
    } else {
      m_send_count++;

      //
      // add data to the to dest buffer
      if (m_vec_send_buffers[dest].empty()) {
        m_send_dest_queue.push_back(dest);
      }
      size_t bytes = pack_lambda(m_vec_send_buffers[dest],
                                 std::forward<const SendArgs>(args)...);
      m_local_bytes_sent += bytes;
      m_send_buffer_bytes += bytes;

      //
      // Check if send buffer capacity has been exceeded
      while (m_send_buffer_bytes > m_buffer_capacity_bytes) {
        ASSERT_DEBUG(!m_send_dest_queue.empty());
        int dest = m_send_dest_queue.front();
        m_send_dest_queue.pop_front();
        flush_send_buffer(dest);
      }
    }
    // If not experiencing recursion, check if listener has queued receives to
    // process
    if (recursion_detector == 1 && receive_queue_peek_size() > 0) {
      process_receive_queue();
    }
    --recursion_detector;
  }

  template <typename... SendArgs>
  void async_bcast(const SendArgs &...args) {
    for (int dest = 0; dest < m_comm_size; ++dest) {
      async(dest, std::forward<const SendArgs>(args)...);
    }
  }

  template <typename... SendArgs>
  void async_mcast(const std::vector<int> &dests, const SendArgs &...args) {
    for (auto dest : dests) {
      async(dest, std::forward<const SendArgs>(args)...);
    }
  }

  /**
   * @brief Control Flow Barrier
   * Only blocks the control flow until all processes in the communicator have
   * called it. See:  MPI_Barrier()
   */
  void cf_barrier() { ASSERT_MPI(MPI_Barrier(m_comm_barrier)); }

  /**
   * @brief Full communicator barrier
   *
   */
  void barrier() {
    flush_all_local_and_process_incoming();
    std::pair<uint64_t, uint64_t> previous_counts{1, 2};
    std::pair<uint64_t, uint64_t> current_counts{3, 4};
    while (!(current_counts.first == current_counts.second &&
             previous_counts == current_counts)) {
      previous_counts = current_counts;
      current_counts  = barrier_reduce_counts();
    }
    ASSERT_RELEASE(m_pre_barrier_callbacks.empty());
    ASSERT_RELEASE(m_send_dest_queue.empty());
  }

  /**
   * @brief Registers a callback that will be executed prior to the barrier
   * completion
   *
   * @param fn callback function
   */
  void register_pre_barrier_callback(const std::function<void()> &fn) {
    m_pre_barrier_callbacks.push_back(fn);
  }

  template <typename T>
  ygm_ptr<T> make_ygm_ptr(T &t) {
    ygm_ptr<T> to_return(&t);
    to_return.check(*this);
    return to_return;
  }

  int64_t local_bytes_sent() const { return m_local_bytes_sent; }

  void reset_bytes_sent_counter() { m_local_bytes_sent = 0; }

  int64_t local_rpc_calls() const { return m_local_rpc_calls; }

  void reset_rpc_call_counter() { m_local_rpc_calls = 0; }

  template <typename T>
  T all_reduce_sum(const T &t) const {
    T to_return;
    ASSERT_MPI(MPI_Allreduce(&t, &to_return, 1, detail::mpi_typeof(T()),
                             MPI_SUM, m_comm_other));
    return to_return;
  }

  template <typename T>
  T all_reduce_min(const T &t) const {
    T to_return;
    ASSERT_MPI(MPI_Allreduce(&t, &to_return, 1, detail::mpi_typeof(T()),
                             MPI_MIN, m_comm_other));
    return to_return;
  }

  template <typename T>
  T all_reduce_max(const T &t) const {
    T to_return;
    ASSERT_MPI(MPI_Allreduce(&t, &to_return, 1, detail::mpi_typeof(T()),
                             MPI_MAX, m_comm_other));
    return to_return;
  }

  template <typename T>
  void mpi_send(const T &data, int dest, int tag, MPI_Comm comm) const {
    std::vector<std::byte>   packed;
    cereal::YGMOutputArchive oarchive(packed);
    oarchive(data);
    size_t packed_size = packed.size();
    ASSERT_RELEASE(packed_size < 1024 * 1024 * 1024);
    ASSERT_MPI(MPI_Send(&packed_size, 1, detail::mpi_typeof(packed_size), dest,
                        tag, comm));
    ASSERT_MPI(MPI_Send(packed.data(), packed_size, MPI_BYTE, dest, tag, comm));
  }

  template <typename T>
  T mpi_recv(int source, int tag, MPI_Comm comm) const {
    std::vector<std::byte> packed;
    size_t                 packed_size{0};
    ASSERT_MPI(MPI_Recv(&packed_size, 1, detail::mpi_typeof(packed_size),
                        source, tag, comm, MPI_STATUS_IGNORE));
    packed.resize(packed_size);
    ASSERT_MPI(MPI_Recv(packed.data(), packed_size, MPI_BYTE, source, tag, comm,
                        MPI_STATUS_IGNORE));

    T                       to_return;
    cereal::YGMInputArchive iarchive(packed.data(), packed.size());
    iarchive(to_return);
    return to_return;
  }

  template <typename T>
  T mpi_bcast(const T &to_bcast, int root, MPI_Comm comm) const {
    std::vector<std::byte>   packed;
    cereal::YGMOutputArchive oarchive(packed);
    if (rank() == root) {
      oarchive(to_bcast);
    }
    size_t packed_size = packed.size();
    ASSERT_RELEASE(packed_size < 1024 * 1024 * 1024);
    ASSERT_MPI(MPI_Bcast(&packed_size, 1, detail::mpi_typeof(packed_size), root,
                         comm));
    if (rank() != root) {
      packed.resize(packed_size);
    }
    ASSERT_MPI(MPI_Bcast(packed.data(), packed_size, MPI_BYTE, root, comm));

    cereal::YGMInputArchive iarchive(packed.data(), packed.size());
    T                       to_return;
    iarchive(to_return);
    return to_return;
  }

  /**
   * @brief Tree based reduction, could be optimized significantly
   *
   * @tparam T
   * @tparam MergeFunction
   * @param in
   * @param merge
   * @return T
   */
  template <typename T, typename MergeFunction>
  T all_reduce(const T &in, MergeFunction merge) const {
    int first_child  = 2 * rank() + 1;
    int second_child = 2 * (rank() + 1);
    int parent       = (rank() - 1) / 2;

    // Step 1: Receive from children, merge into tmp
    T tmp = in;
    if (first_child < size()) {
      T fc = mpi_recv<T>(first_child, 0, m_comm_other);
      tmp  = merge(tmp, fc);
    }
    if (second_child < size()) {
      T sc = mpi_recv<T>(second_child, 0, m_comm_other);
      tmp  = merge(tmp, sc);
    }

    // Step 2: Send merged to parent
    if (rank() != 0) {
      mpi_send(tmp, parent, 0, m_comm_other);
    }

    // Step 3:  Rank 0 bcasts
    T to_return = mpi_bcast(tmp, 0, m_comm_other);
    return to_return;
  }

  const detail::layout &layout() const { return m_layout; }

 private:
  std::pair<uint64_t, uint64_t> barrier_reduce_counts() {
    uint64_t local_counts[2]  = {m_recv_count, m_send_count};
    uint64_t global_counts[2] = {0, 0};

    MPI_Request req = MPI_REQUEST_NULL;
    ASSERT_MPI(MPI_Iallreduce(local_counts, global_counts, 2, MPI_UINT64_T,
                              MPI_SUM, m_comm_barrier, &req));
    int mpi_test_flag{0};
    while (!mpi_test_flag) {
      flush_all_local_and_process_incoming();
      ASSERT_MPI(MPI_Test(&req, &mpi_test_flag, MPI_STATUS_IGNORE));
    }
    return {global_counts[0], global_counts[1]};
  }

  /**
   * @brief Flushes send buffer to dest
   *
   * @param dest
   */
  void flush_send_buffer(int dest) {
    ASSERT_RELEASE(dest != m_comm_rank);
    if (m_vec_send_buffers[dest].size() > 0) {
      ASSERT_MPI(MPI_Send(m_vec_send_buffers[dest].data(),
                          m_vec_send_buffers[dest].size(), MPI_BYTE, dest, 0,
                          m_comm_async));
      m_send_buffer_bytes -= m_vec_send_buffers[dest].size();
    }
    m_vec_send_buffers[dest].clear();
  }

  /**
   * @brief Flushes all local state and buffers.
   * Notifies any registered barrier watchers.
   */
  void flush_all_local_and_process_incoming() {
    // Keep flushing until all local work is complete
    bool did_something = true;
    while (did_something) {
      did_something = process_receive_queue();
      //
      //  Notify registered barrier watchers
      while (!m_pre_barrier_callbacks.empty()) {
        did_something            = true;
        std::function<void()> fn = m_pre_barrier_callbacks.front();
        m_pre_barrier_callbacks.pop_front();
        fn();
      }

      //
      //  Flush each send buffer
      while (!m_send_dest_queue.empty()) {
        did_something = true;
        int dest      = m_send_dest_queue.front();
        m_send_dest_queue.pop_front();
        flush_send_buffer(dest);
        process_receive_queue();
      }
    }
  }

  /**
   * @brief Listener thread
   *
   */
  void listener_thread() {
    while (true) {
      MPI_Status status;
      ASSERT_MPI(MPI_Probe(MPI_ANY_SOURCE, MPI_ANY_TAG, m_comm_async, &status));

      int count{0};
      ASSERT_MPI(MPI_Get_count(&status, MPI_BYTE, &count));

      int source = status.MPI_SOURCE;
      int tag    = status.MPI_TAG;

      std::shared_ptr<std::byte[]> recv_buffer{new std::byte[count]};

      ASSERT_MPI(MPI_Recv(recv_buffer.get(), count, MPI_BYTE, source, tag,
                          m_comm_async, &status));

      // Check for kill signal
      if (status.MPI_SOURCE == m_comm_rank) break;

      receive_queue_push_back(recv_buffer, count);
    }
  }

  size_t receive_queue_peek_size() const { return m_recv_queue.size(); }

  std::pair<std::shared_ptr<std::byte[]>, size_t> receive_queue_try_pop() {
    std::scoped_lock lock(m_recv_queue_mutex);
    if (m_recv_queue.empty()) {
      ASSERT_RELEASE(m_recv_queue_bytes == 0);
      return std::make_pair(std::shared_ptr<std::byte[]>{}, size_t{0});
    } else {
      auto to_return = m_recv_queue.front();
      m_recv_queue.pop_front();
      m_recv_queue_bytes -= to_return.second;
      return to_return;
    }
  }

  void receive_queue_push_back(const std::shared_ptr<std::byte[]> &b,
                               size_t                              size) {
    {
      std::scoped_lock lock(m_recv_queue_mutex);
      m_recv_queue.push_back({b, size});
      m_recv_queue.size();
      m_recv_queue_bytes += size;
    }
    if (m_recv_queue_bytes > m_buffer_capacity_bytes) {
      // Sleep a microsecond for every KB over.
      std::this_thread::sleep_for(std::chrono::microseconds(
          (m_recv_queue_bytes - m_buffer_capacity_bytes) / 1024));
    }
  }

  // Used if dest = m_comm_rank
  template <typename Lambda, typename... Args>
  int32_t local_receive(Lambda l, const Args &...args) {
    ASSERT_DEBUG(sizeof(Lambda) == 1);
    // Question: should this be std::forward(...)
    // \pp was: (l)(this, m_comm_rank, args...);
    ygm::meta::apply_optional(l, std::make_tuple(this),
                              std::make_tuple(args...));
    return 1;
  }

  template <typename Lambda, typename... PackArgs>
  size_t pack_lambda(std::vector<std::byte> &packed, Lambda l,
                     const PackArgs &...args) {
    size_t                        size_before = packed.size();
    const std::tuple<PackArgs...> tuple_args(
        std::forward<const PackArgs>(args)...);
    ASSERT_DEBUG(sizeof(Lambda) == 1);

    void (*fun_ptr)(comm *, cereal::YGMInputArchive &) =
        [](comm *c, cereal::YGMInputArchive &bia) {
          std::tuple<PackArgs...> ta;
          bia(ta);
          Lambda *pl = nullptr;
          auto    t1 = std::make_tuple((comm *)c);

          // \pp was: std::apply(*pl, std::tuple_cat(t1, ta));
          ygm::meta::apply_optional(*pl, std::move(t1), std::move(ta));
        };

    cereal::YGMOutputArchive oarchive(packed);  // Create an output archive
                                                // // oarchive(fun_ptr);
    int64_t iptr = (int64_t)fun_ptr - (int64_t)&reference;
    oarchive(iptr, tuple_args);
    return packed.size() - size_before;
  }

  /**
   * @brief Static reference point to anchor address space randomization.
   *
   */
  static void reference() {}

  /**
   * @brief Process receive queue of messages received by the listener thread.
   *
   * @return True if receive queue was non-empty, else false
   */
  bool process_receive_queue() {
    bool received = false;
    comm tmp_comm(shared_from_this());
    while (true) {
      auto buffer = receive_queue_try_pop();
      if (buffer.second == 0) break;
      received = true;
      cereal::YGMInputArchive iarchive(buffer.first.get(), buffer.second);
      while (!iarchive.empty()) {
        int64_t iptr;
        iarchive(iptr);
        iptr += (int64_t)&reference;
        void (*fun_ptr)(comm *, cereal::YGMInputArchive &);
        memcpy(&fun_ptr, &iptr, sizeof(uint64_t));
        fun_ptr(&tmp_comm, iarchive);
        m_recv_count++;
        m_local_rpc_calls++;
      }
    }
    return received;
  }

  MPI_Comm m_comm_async;
  MPI_Comm m_comm_barrier;
  MPI_Comm m_comm_other;
  int      m_comm_size;
  int      m_comm_rank;
  size_t   m_buffer_capacity_bytes;

  detail::layout m_layout;

  std::vector<std::vector<std::byte>> m_vec_send_buffers;
  size_t                              m_send_buffer_bytes = 0;
  std::deque<int>                     m_send_dest_queue;

  std::deque<std::pair<std::shared_ptr<std::byte[]>, size_t>> m_recv_queue;
  std::mutex m_recv_queue_mutex;
  size_t     m_recv_queue_bytes = 0;

  std::thread m_listener;

  std::deque<std::function<void()>> m_pre_barrier_callbacks;

  uint64_t m_recv_count = 0;
  uint64_t m_send_count = 0;

  int64_t m_local_rpc_calls  = 0;
  int64_t m_local_bytes_sent = 0;
};

inline comm::comm(int *argc, char ***argv,
                  int buffer_capacity = 16 * 1024 * 1024) {
  pimpl_if = std::make_shared<detail::mpi_init_finalize>(argc, argv);
  pimpl    = std::make_shared<comm::impl>(MPI_COMM_WORLD, buffer_capacity);
}

inline comm::comm(MPI_Comm mcomm, int buffer_capacity = 16 * 1024 * 1024) {
  pimpl_if.reset();
  int flag(0);
  ASSERT_MPI(MPI_Initialized(&flag));
  if (!flag) {
    throw std::runtime_error("ERROR: MPI not initialized");
  }
  int provided(0);
  ASSERT_MPI(MPI_Query_thread(&provided));
  if (provided != MPI_THREAD_MULTIPLE) {
    throw std::runtime_error("ERROR: MPI_THREAD_MULTIPLE not provided");
  }
  pimpl = std::make_shared<comm::impl>(mcomm, buffer_capacity);
}

inline comm::comm(std::shared_ptr<impl> impl_ptr) : pimpl(impl_ptr) {}

inline comm::~comm() {
  if (pimpl.use_count() == 1) {
    barrier();
  }
  pimpl.reset();
  pimpl_if.reset();
}

template <typename AsyncFunction, typename... SendArgs>
inline void comm::async(int dest, AsyncFunction fn, const SendArgs &...args) {
  static_assert(std::is_empty<AsyncFunction>::value,
                "Only stateless lambdas are supported");
  pimpl->async(dest, fn, std::forward<const SendArgs>(args)...);
}

template <typename AsyncFunction, typename... SendArgs>
inline void comm::async_bcast(AsyncFunction fn, const SendArgs &...args) {
  static_assert(std::is_empty<AsyncFunction>::value,
                "Only stateless lambdas are supported");
  pimpl->async_bcast(fn, std::forward<const SendArgs>(args)...);
}

template <typename AsyncFunction, typename... SendArgs>
inline void comm::async_mcast(const std::vector<int> &dests, AsyncFunction fn,
                              const SendArgs &...args) {
  static_assert(std::is_empty<AsyncFunction>::value,
                "Only stateless lambdas are supported");
  pimpl->async_mcast(dests, fn, std::forward<const SendArgs>(args)...);
}

inline const detail::layout &comm::layout() const { return pimpl->layout(); }

inline int comm::size() const { return pimpl->size(); }
inline int comm::rank() const { return pimpl->rank(); }

inline int64_t comm::local_bytes_sent() const {
  return pimpl->local_bytes_sent();
}

inline int64_t comm::global_bytes_sent() const {
  return all_reduce_sum(local_bytes_sent());
}

inline void comm::reset_bytes_sent_counter() {
  pimpl->reset_bytes_sent_counter();
}

inline int64_t comm::local_rpc_calls() const {
  return pimpl->local_rpc_calls();
}

inline int64_t comm::global_rpc_calls() const {
  return all_reduce_sum(local_rpc_calls());
}

inline void comm::reset_rpc_call_counter() { pimpl->reset_rpc_call_counter(); }

inline void comm::barrier() { pimpl->barrier(); }

inline void comm::cf_barrier() { pimpl->cf_barrier(); }

template <typename T>
inline ygm_ptr<T> comm::make_ygm_ptr(T &t) {
  return pimpl->make_ygm_ptr(t);
}

inline void comm::register_pre_barrier_callback(
    const std::function<void()> &fn) {
  pimpl->register_pre_barrier_callback(fn);
}

template <typename T>
inline T comm::all_reduce_sum(const T &t) const {
  return pimpl->all_reduce_sum(t);
}

template <typename T>
inline T comm::all_reduce_min(const T &t) const {
  return pimpl->all_reduce_min(t);
}

template <typename T>
inline T comm::all_reduce_max(const T &t) const {
  return pimpl->all_reduce_max(t);
}

template <typename T, typename MergeFunction>
inline T comm::all_reduce(const T &t, MergeFunction merge) {
  return pimpl->all_reduce(t, merge);
}

}  // namespace ygm
