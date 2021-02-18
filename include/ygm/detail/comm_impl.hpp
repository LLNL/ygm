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

#include <ygm/detail/mpi.hpp>
#include <ygm/detail/ygm_cereal_archive.hpp>
#include <ygm/utility.hpp>

#include <omp.h>

namespace ygm {

template <typename SendBufferManager> class comm<SendBufferManager>::impl {
public:
  impl(MPI_Comm c, int num_listeners = -1, int buffer_capacity = 1048576) {
    ASSERT_MPI(MPI_Comm_dup(c, &m_comm_barrier));
    ASSERT_MPI(MPI_Comm_dup(c, &m_comm_other));
    ASSERT_MPI(MPI_Comm_size(c, &m_comm_size));
    ASSERT_MPI(MPI_Comm_rank(c, &m_comm_rank));
    m_buffer_capacity = buffer_capacity;

    if (num_listeners < 0) {
      m_num_listeners = omp_get_max_threads();
    } else {
      m_num_listeners = num_listeners;
    }

    // Need to use same number of listeners on all ranks
    m_num_listeners = all_reduce_min(m_num_listeners);

    m_vec_comms_async.resize(m_num_listeners);
    for (int i = 0; i < m_num_listeners; ++i) {
      ASSERT_MPI(MPI_Comm_dup(c, &m_vec_comms_async[i]));
    }

    // Create send buffer manager
    m_send_buffer_manager =
        SendBufferManager(m_comm_size, buffer_capacity, this);

    // launch listener threads
    for (int i = 0; i < m_num_listeners; ++i) {
      m_vec_listeners.push_back(
          std::thread(&impl::listen, this, m_vec_comms_async[i]));
    }
  }

  ~impl() {
    barrier();
    // send kill signal to self (listener thread)
    for (int i = 0; i < m_num_listeners; ++i) {
      MPI_Send(NULL, 0, MPI_BYTE, m_comm_rank, 1, m_vec_comms_async[i]);
    }

    // Join listener thread.
    for (int i = 0; i < m_num_listeners; ++i) {
      m_vec_listeners[i].join();
    }

    // Free cloned communicator.
    ASSERT_RELEASE(MPI_Barrier(m_comm_other) == MPI_SUCCESS);
    for (int i = 0; i < m_vec_comms_async.size(); ++i) {
      MPI_Comm_free(&m_vec_comms_async[i]);
    }
    MPI_Comm_free(&m_comm_barrier);
    MPI_Comm_free(&m_comm_other);
  }

  int size() const { return m_comm_size; }
  int rank() const { return m_comm_rank; }

  template <typename... SendArgs>
  void async(int dest, const SendArgs &... args) {
    ASSERT_DEBUG(dest < m_comm_size);
    m_send_count++;
    std::vector<char> data = pack_lambda(std::forward<const SendArgs>(args)...);

    if (data.size() < m_buffer_capacity) {
      m_send_buffer_manager.insert(dest, data);
    } else {
      send_large_message(data, dest);
    }
  }

  // Blocking barrier
  void barrier() {
    int64_t all_count = -1;
    while (all_count != 0) {
      async_flush_all();

      int64_t local_count = m_send_count - m_recv_count;

      ASSERT_MPI(MPI_Allreduce(&local_count, &all_count, 1, MPI_INT64_T,
                               MPI_SUM, m_comm_barrier));
      std::this_thread::yield();
    }
  }

  void async_send(const int dest, const int size, const char *buffer) {
    ASSERT_MPI(
        MPI_Send(buffer, size, MPI_BYTE, dest, 0,
                 m_vec_comms_async[++m_send_comm_index % m_num_listeners]));
  }

  void async_flush_all() {
    m_send_buffer_manager.all_flush();
    // TODO async_flush_bcast(); goes here
  }

  template <typename T> T all_reduce_sum(const T &t) const {
    T to_return;
    ASSERT_MPI(MPI_Allreduce(&t, &to_return, 1, detail::mpi_typeof(T()),
                             MPI_SUM, m_comm_other));
    return to_return;
  }

  template <typename T> T all_reduce_min(const T &t) const {
    T to_return;
    ASSERT_MPI(MPI_Allreduce(&t, &to_return, 1, detail::mpi_typeof(T()),
                             MPI_MIN, m_comm_other));
    return to_return;
  }

  template <typename T> T all_reduce_max(const T &t) const {
    T to_return;
    ASSERT_MPI(MPI_Allreduce(&t, &to_return, 1, detail::mpi_typeof(T()),
                             MPI_MAX, m_comm_other));
    return to_return;
  }

  template <typename T>
  void mpi_send(const T &data, int dest, int tag, MPI_Comm comm) const {
    std::vector<char> packed;
    cereal::YGMOutputArchive oarchive(packed);
    oarchive(data);
    size_t packed_size = packed.size();
    ASSERT_RELEASE(packed_size < 1024 * 1024 * 1024);
    ASSERT_MPI(MPI_Send(&packed_size, 1, detail::mpi_typeof(packed_size), dest,
                        tag, comm));
    ASSERT_MPI(MPI_Send(packed.data(), packed_size, MPI_BYTE, dest, tag, comm));
  }

  template <typename T> T mpi_recv(int source, int tag, MPI_Comm comm) const {
    std::vector<char> packed;
    size_t packed_size{0};
    ASSERT_MPI(MPI_Recv(&packed_size, 1, detail::mpi_typeof(packed_size),
                        source, tag, comm, MPI_STATUS_IGNORE));
    packed.resize(packed_size);
    ASSERT_MPI(MPI_Recv(packed.data(), packed_size, MPI_BYTE, source, tag, comm,
                        MPI_STATUS_IGNORE));

    T to_return;
    cereal::YGMInputArchive iarchive(packed.data(), packed.size());
    iarchive(to_return);
    return to_return;
  }

  template <typename T>
  T mpi_bcast(const T &to_bcast, int root, MPI_Comm comm) const {
    std::vector<char> packed;
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
    T to_return;
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
    int first_child = 2 * rank() + 1;
    int second_child = 2 * (rank() + 1);
    int parent = (rank() - 1) / 2;

    // Step 1: Receive from children, merge into tmp
    T tmp = in;
    if (first_child < size()) {
      T fc = mpi_recv<T>(first_child, 0, m_comm_other);
      tmp = merge(tmp, fc);
    }
    if (second_child < size()) {
      T sc = mpi_recv<T>(second_child, 0, m_comm_other);
      tmp = merge(tmp, sc);
    }

    // Step 2: Send merged to parent
    if (rank() != 0) {
      mpi_send(tmp, parent, 0, m_comm_other);
    }

    // Step 3:  Rank 0 bcasts
    T to_return = mpi_bcast(tmp, 0, m_comm_other);
    return to_return;
  }

private:
  /**
   * @brief Listener thread
   *
   */
  void listen(MPI_Comm c) {
    // Set up 2 buffers for nonblocking receives
    std::unique_ptr<std::vector<char>> recv_buffer =
        std::make_unique<std::vector<char>>(m_buffer_capacity);
    std::unique_ptr<std::vector<char>> work_buffer =
        std::make_unique<std::vector<char>>(m_buffer_capacity);
    // recv_buffer->resize(m_buffer_capacity);
    // work_buffer->resize(m_buffer_capacity);

    // Post initial as blocking. Can't do work I haven't received yet.
    MPI_Status status;
    ASSERT_MPI(MPI_Recv(recv_buffer->data(), m_buffer_capacity, MPI_BYTE,
                        MPI_ANY_SOURCE, MPI_ANY_TAG, c, &status));
    while (true) {
      if (status.MPI_TAG == large_message_announce_tag) {
        // Determine size and source of message
        size_t size = *(reinterpret_cast<size_t *>(recv_buffer->data()));
        int src = status.MPI_SOURCE;

        // Allocate large buffer
        auto large_recv_buff = std::make_shared<std::vector<char>>(size);

        // Receive large message
        receive_large_message(large_recv_buff, src, size, c);

        // Post nonblocking receive
        MPI_Request req;
        ASSERT_MPI(MPI_Irecv(recv_buffer->data(), m_buffer_capacity, MPI_BYTE,
                             MPI_ANY_SOURCE, MPI_ANY_TAG, c, &req));

        process_buffer(*large_recv_buff, status.MPI_SOURCE);

        MPI_Wait(&req, &status);
      } else {
        // Check for kill signal
        if (status.MPI_SOURCE == m_comm_rank && status.MPI_TAG == 1)
          break;

        std::swap(recv_buffer, work_buffer);

        recv_buffer->resize(m_buffer_capacity);

        // Post nonblocking receive
        MPI_Request req;
        ASSERT_MPI(MPI_Irecv(recv_buffer->data(), m_buffer_capacity, MPI_BYTE,
                             MPI_ANY_SOURCE, MPI_ANY_TAG, c, &req));

        int count;
        ASSERT_MPI(MPI_Get_count(&status, MPI_BYTE, &count))
        // std::cout << "RANK: " << rank() << " received count: " << count
        //           << std::endl;
        // Resize buffer to cout MPI actually received
        work_buffer->resize(count);

        process_buffer(*work_buffer, status.MPI_SOURCE);

        MPI_Wait(&req, &status);
      }
    }
  }

  /*
   * @brief Send a large message
   *
   * @param dest Destination for message
   * @param msg Packed message to send
   */
  void send_large_message(const std::vector<char> &msg, const int dest) {
    // Announce the large message and its size
    size_t size = msg.size();
    ASSERT_MPI(MPI_Send(&size, 8, MPI_BYTE, dest, large_message_announce_tag,
                        m_vec_comms_async[++large_message_comm_index]));

    // Send message
    ASSERT_MPI(MPI_Send(msg.data(), size, MPI_BYTE, dest, large_message_tag,
                        m_vec_comms_async[large_message_comm_index]));
  }

  /*
   * @brief Receive a large message that has been announced
   *
   * @param src Source of message
   * @param msg Buffer to hold message
   */
  void receive_large_message(std::shared_ptr<std::vector<char>> msg,
                             const int src, const size_t size,
                             const MPI_Comm c) {
    ASSERT_MPI(MPI_Recv(msg->data(), size, MPI_BYTE, src, large_message_tag, c,
                        MPI_STATUS_IGNORE));
  }

  // Used if dest = m_comm_rank
  template <typename Lambda, typename... Args>
  int32_t local_receive(Lambda l, const Args &... args) {
    ASSERT_DEBUG(sizeof(Lambda) == 1);
    // Question: should this be std::forward(...)
    (l)(this, m_comm_rank, args...);
    return 1;
  }

  template <typename Lambda, typename... PackArgs>
  std::vector<char> pack_lambda(Lambda l, const PackArgs &... args) {
    std::vector<char> to_return;
    const std::tuple<PackArgs...> tuple_args(
        std::forward<const PackArgs>(args)...);
    ASSERT_DEBUG(sizeof(Lambda) == 1);

    void (*fun_ptr)(impl *, int, cereal::YGMInputArchive &) =
        [](impl *t, int from, cereal::YGMInputArchive &bia) {
          std::tuple<PackArgs...> ta;
          bia(ta);
          Lambda *pl;
          auto t1 = std::make_tuple((impl *)t, from);
          std::apply(*pl, std::tuple_cat(t1, ta));
        };

    cereal::YGMOutputArchive oarchive(to_return); // Create an output archive
                                                  // // oarchive(fun_ptr);
    int64_t iptr = (int64_t)fun_ptr - (int64_t)&reference;
    oarchive(iptr, tuple_args);

    return to_return;
  }

  // this is used to fix address space randomization
  static void reference() {}

  void process_buffer(std::vector<char> &buffer, const int from) {
    cereal::YGMInputArchive iarchive(buffer.data(), buffer.size());
    while (!iarchive.empty()) {
      int64_t iptr;
      iarchive(iptr);
      iptr += (int64_t)&reference;
      void (*fun_ptr)(impl *, int, cereal::YGMInputArchive &);
      memcpy(&fun_ptr, &iptr, sizeof(uint64_t));
      fun_ptr(this, from, iarchive);
      m_recv_count++;
    }
  }

  std::vector<MPI_Comm> m_vec_comms_async;
  MPI_Comm m_comm_barrier;
  MPI_Comm m_comm_other;
  int m_comm_size;
  int m_comm_rank;
  size_t m_buffer_capacity;
  int m_num_listeners;

  SendBufferManager m_send_buffer_manager;

  std::vector<std::thread> m_vec_listeners;

  std::atomic<int64_t> m_recv_count = 0;
  std::atomic<int64_t> m_send_count = 0;

  std::atomic<size_t> m_send_comm_index = 0;

  int large_message_announce_tag = 32766;
  int large_message_tag = 32767;
  std::atomic<int> large_message_comm_index = 0;
};

template <typename SendBufferManager>
inline comm<SendBufferManager>::comm(int *argc, char ***argv, int num_listeners,
                                     int buffer_capacity) {
  pimpl_if = std::make_shared<detail::mpi_init_finalize>(argc, argv);
  pimpl = std::make_shared<comm::impl>(MPI_COMM_WORLD, num_listeners,
                                       buffer_capacity);
}

template <typename SendBufferManager>
inline comm<SendBufferManager>::comm(MPI_Comm mcomm, int num_listeners,
                                     int buffer_capacity) {
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
  pimpl = std::make_shared<comm::impl>(mcomm, num_listeners, buffer_capacity);
}

template <typename SendBufferManager> inline comm<SendBufferManager>::~comm() {
  ASSERT_RELEASE(MPI_Barrier(MPI_COMM_WORLD) == MPI_SUCCESS);
  pimpl.reset();
  ASSERT_RELEASE(MPI_Barrier(MPI_COMM_WORLD) == MPI_SUCCESS);
  pimpl_if.reset();
}

template <typename SendBufferManager>
template <typename AsyncFunction, typename... SendArgs>
inline void comm<SendBufferManager>::async(int dest, AsyncFunction fn,
                                           const SendArgs &... args) {
  static_assert(std::is_empty<AsyncFunction>::value,
                "Only stateless lambdas are supported");
  pimpl->async(dest, fn, std::forward<const SendArgs>(args)...);
}

template <typename SendBufferManager>
inline int comm<SendBufferManager>::size() const {
  return pimpl->size();
}
template <typename SendBufferManager>
inline int comm<SendBufferManager>::rank() const {
  return pimpl->rank();
}

template <typename SendBufferManager>
inline void comm<SendBufferManager>::barrier() {
  pimpl->barrier();
}

template <typename SendBufferManager>
inline void comm<SendBufferManager>::async_flush(int rank) {
  pimpl->async_flush(rank);
}

template <typename SendBufferManager>
inline void comm<SendBufferManager>::async_flush_all() {
  pimpl->async_flush_all();
}

template <typename SendBufferManager>
template <typename T>
inline T comm<SendBufferManager>::all_reduce_sum(const T &t) const {
  return pimpl->all_reduce_sum(t);
}

template <typename SendBufferManager>
template <typename T>
inline T comm<SendBufferManager>::all_reduce_min(const T &t) const {
  return pimpl->all_reduce_min(t);
}

template <typename SendBufferManager>
template <typename T>
inline T comm<SendBufferManager>::all_reduce_max(const T &t) const {
  return pimpl->all_reduce_max(t);
}

template <typename SendBufferManager>
template <typename T, typename MergeFunction>
inline T comm<SendBufferManager>::all_reduce(const T &t, MergeFunction merge) {
  return pimpl->all_reduce(t, merge);
}

} // namespace ygm
