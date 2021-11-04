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
#include <ygm/meta/functional.hpp>

namespace ygm {

class comm::impl {
 public:
  impl(MPI_Comm c, int buffer_capacity) {
    ASSERT_MPI(MPI_Comm_dup(c, &m_comm_async));
    ASSERT_MPI(MPI_Comm_dup(c, &m_comm_barrier));
    ASSERT_MPI(MPI_Comm_dup(c, &m_comm_other));
    ASSERT_MPI(MPI_Comm_size(m_comm_async, &m_comm_size));
    ASSERT_MPI(MPI_Comm_rank(m_comm_async, &m_comm_rank));
    m_buffer_capacity = buffer_capacity;

    // Allocate send buffers
    for (int i = 0; i < m_comm_size; ++i) {
      m_vec_send_buffers.push_back(allocate_buffer());
    }

    // launch listener thread
    m_listener = std::thread(&impl::listen, this);
  }

  ~impl() {
    barrier();
    // send kill signal to self (listener thread)
    MPI_Send(NULL, 0, MPI_BYTE, m_comm_rank, 0, m_comm_async);
    // Join listener thread.
    m_listener.join();
    // Free cloned communicator.
    ASSERT_RELEASE(MPI_Barrier(m_comm_async) == MPI_SUCCESS);
    MPI_Comm_free(&m_comm_async);
    MPI_Comm_free(&m_comm_barrier);
    MPI_Comm_free(&m_comm_other);
  }

  int size() const { return m_comm_size; }
  int rank() const { return m_comm_rank; }

  template <typename... SendArgs>
  void async(int dest, const SendArgs &... args) {
    ASSERT_DEBUG(dest < m_comm_size);
    if (dest == m_comm_rank) {
      local_receive(std::forward<const SendArgs>(args)...);
    } else {
      m_send_count++;
      std::vector<char> data =
          pack_lambda(std::forward<const SendArgs>(args)...);
      m_local_bytes_sent += data.size();

      if (data.size() < m_buffer_capacity) {
        // check if buffer doesn't have enough space
        if (data.size() + m_vec_send_buffers[dest]->size() >
            m_buffer_capacity) {
          async_flush(dest);
        }

        // add data to the to dest buffer
        m_vec_send_buffers[dest]->insert(m_vec_send_buffers[dest]->end(),
                                         data.begin(), data.end());
      } else {  // Large message
        send_large_message(data, dest);
      }
    }
    // check if listener has queued receives to process
    if (receive_queue_peek_size() > 0) {
      receive_queue_process();
    }
  }

  template <typename... SendArgs>
  void async_preempt(int dest, const SendArgs &... args) {
    async(dest, std::forward<const SendArgs>(args)...);
  }

  template <typename... SendArgs>
  void async_bcast(const SendArgs &... args) {
    for (int dest = 0; dest < m_comm_size; ++dest) {
      async(dest, std::forward<const SendArgs>(args)...);
    }
  }

  template <typename... SendArgs>
  void async_bcast_preempt(const SendArgs &... args) {
    bcast(std::forward<const SendArgs>(args)...);
  }

  template <typename... SendArgs>
  void async_mcast(const std::vector<int> &dests, const SendArgs &... args) {
    for (auto dest : dests) {
      async(dest, std::forward<const SendArgs>(args)...);
    }
  }

  template <typename... SendArgs>
  void async_mcast_preempt(const std::vector<int> &dests,
                           const SendArgs &... args) {
    mcast(dests, std::forward<const SendArgs>(args)...);
  }

  // //
  // // Blocking barrier
  // void barrier() {
  //   int64_t all_count = -1;
  //   while (all_count != 0) {
  //     receive_queue_process();
  //     do {
  //       async_flush_all();
  //       std::this_thread::yield();
  //     } while (receive_queue_process());

  //     int64_t local_count = m_send_count - m_recv_count;

  //     ASSERT_MPI(MPI_Allreduce(&local_count, &all_count, 1, MPI_INT64_T,
  //                              MPI_SUM, m_comm_barrier));
  //     std::this_thread::yield();
  //     // std::cout << "MPI_Allreduce() " << std::endl;
  //   }
  // }

  void wait_local_idle() {
    receive_queue_process();
    do {
      async_flush_all();
      std::this_thread::yield();
    } while (receive_queue_process());
  }

  void barrier() {
    while (true) {
      wait_local_idle();
      MPI_Request req = MPI_REQUEST_NULL;
      int64_t     first_all_count{-1};
      int64_t     first_local_count = m_send_count - m_recv_count;
      ASSERT_MPI(MPI_Iallreduce(&first_local_count, &first_all_count, 1,
                                MPI_INT64_T, MPI_SUM, m_comm_barrier, &req));

      while (true) {
        int test_flag{-1};
        ASSERT_MPI(MPI_Test(&req, &test_flag, MPI_STATUS_IGNORE));
        if (test_flag) {
          if (first_all_count == 0) {
            // double check
            int64_t second_all_count{-1};
            int64_t second_local_count = m_send_count - m_recv_count;
            ASSERT_MPI(MPI_Allreduce(&second_local_count, &second_all_count, 1,
                                     MPI_INT64_T, MPI_SUM, m_comm_barrier));
            if (second_all_count == 0) {
              ASSERT_RELEASE(first_local_count == second_local_count);
              return;
            }
          }
          break;  // failed, start over
        } else {
          wait_local_idle();
        }
      }
    }
  }

  // //  SOMETHING WRONG :(
  // // Non-blocking barrier loop
  // void barrier() {
  //   std::pair<int64_t, int64_t> last{-1, -2}, current{-3, -4}, local{-5,
  //   -6}; MPI_Request req = MPI_REQUEST_NULL;

  //   do {
  //     receive_queue_process();
  //     do { async_flush_all(); } while (receive_queue_process());

  //     int64_t local_count = m_send_count - m_recv_count;

  //     if (req == MPI_REQUEST_NULL) {
  //       last = current;
  //       current = {-3, -4};
  //       local = std::make_pair(m_send_count, m_recv_count);
  //       ASSERT_MPI(MPI_Iallreduce(&local, &current, 2, MPI_INT64_T,
  //       MPI_SUM,
  //                                 m_comm_barrier, &req));
  //     } else {
  //       int flag{-1};
  //       ASSERT_MPI(MPI_Test(&req, &flag, MPI_STATUS_IGNORE));
  //       if (flag) {
  //         req = MPI_REQUEST_NULL;
  //       } else {
  //         std::this_thread::yield();
  //       }
  //     }
  //   } while (req != MPI_REQUEST_NULL || current.first != current.second ||
  //            last != current);
  //   ASSERT_MPI(MPI_Barrier(m_comm_barrier));
  // }

  void async_flush(int dest) {
    if (dest != m_comm_rank) {
      // Skip dest == m_comm_rank;   Only kill messages go to self.
      if (m_vec_send_buffers[dest]->size() == 0) return;
      auto buffer = allocate_buffer();
      std::swap(buffer, m_vec_send_buffers[dest]);
      ASSERT_MPI(MPI_Send(buffer->data(), buffer->size(), MPI_BYTE, dest, 0,
                          m_comm_async));
      free_buffer(buffer);
    }
  }

  void async_flush_all() {
    for (int i = 0; i < size(); ++i) {
      int dest = (rank() + i) % size();
      async_flush(dest);
    }
    // TODO async_flush_bcast(); goes here
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
    std::vector<char>        packed;
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
    std::vector<char> packed;
    size_t            packed_size{0};
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
    std::vector<char>        packed;
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

 private:
  /**
   * @brief Listener thread
   *
   */
  void listen() {
    while (true) {
      auto recv_buffer = allocate_buffer();
      recv_buffer->resize(m_buffer_capacity);  // TODO:  does this clear?
      MPI_Status status;
      ASSERT_MPI(MPI_Recv(recv_buffer->data(), m_buffer_capacity, MPI_BYTE,
                          MPI_ANY_SOURCE, MPI_ANY_TAG, m_comm_async, &status));
      int tag = status.MPI_TAG;

      if (tag == large_message_announce_tag) {
        // Determine size and source of message
        size_t size = *(reinterpret_cast<size_t *>(recv_buffer->data()));
        int    src  = status.MPI_SOURCE;

        // Allocate large buffer
        auto large_recv_buff = std::make_shared<std::vector<char>>(size);

        // Receive large message
        receive_large_message(large_recv_buff, src, size);

        // Add buffer to receive queue
        receive_queue_push_back(large_recv_buff);
      } else {
        int count;
        ASSERT_MPI(MPI_Get_count(&status, MPI_BYTE, &count))
        // std::cout << "RANK: " << rank() << " received count: " << count
        //           << std::endl;
        // Resize buffer to cout MPI actually received
        recv_buffer->resize(count);

        // Check for kill signal
        if (status.MPI_SOURCE == m_comm_rank) break;

        // Add buffer to receive queue
        receive_queue_push_back(recv_buffer);
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
                        m_comm_async));

    // Send message
    ASSERT_MPI(MPI_Send(msg.data(), size, MPI_BYTE, dest, large_message_tag,
                        m_comm_async));
  }

  /*
   * @brief Receive a large message that has been announced
   *
   * @param src Source of message
   * @param msg Buffer to hold message
   */
  void receive_large_message(std::shared_ptr<std::vector<char>> msg,
                             const int src, const size_t size) {
    ASSERT_MPI(MPI_Recv(msg->data(), size, MPI_BYTE, src, large_message_tag,
                        m_comm_async, MPI_STATUS_IGNORE));
  }

  /**
   * @brief Allocates buffer; checks free pool first.
   *
   * @return std::shared_ptr<std::vector<char>>
   */
  std::shared_ptr<std::vector<char>> allocate_buffer() {
    std::scoped_lock lock(m_vec_free_buffers_mutex);
    if (m_vec_free_buffers.empty()) {
      auto to_return = std::make_shared<std::vector<char>>();
      to_return->reserve(m_buffer_capacity);
      return to_return;
    } else {
      auto to_return = m_vec_free_buffers.back();
      m_vec_free_buffers.pop_back();
      return to_return;
    }
  }

  /**
   * @brief Frees a previously allocated buffer.  Adds buffer to free pool.
   *
   * @param b buffer to free
   */
  void free_buffer(std::shared_ptr<std::vector<char>> b) {
    b->clear();
    std::scoped_lock lock(m_vec_free_buffers_mutex);
    m_vec_free_buffers.push_back(b);
  }

  size_t receive_queue_peek_size() const { return m_receive_queue.size(); }

  std::shared_ptr<std::vector<char>> receive_queue_try_pop() {
    std::scoped_lock lock(m_receive_queue_mutex);
    if (m_receive_queue.empty()) {
      return std::shared_ptr<std::vector<char>>();
    } else {
      auto to_return = m_receive_queue.front();
      m_receive_queue.pop_front();
      return to_return;
    }
  }

  void receive_queue_push_back(std::shared_ptr<std::vector<char>> b) {
    size_t current_size = 0;
    {
      std::scoped_lock lock(m_receive_queue_mutex);
      m_receive_queue.push_back(b);
      current_size = m_receive_queue.size();
    }
    if (current_size > 16) {
      std::this_thread::sleep_for(std::chrono::microseconds(current_size - 16));
    }
  }

  // Used if dest = m_comm_rank
  template <typename Lambda, typename... Args>
  int32_t local_receive(Lambda l, const Args &... args) {
    ASSERT_DEBUG(sizeof(Lambda) == 1);
    // Question: should this be std::forward(...)
    // \pp was: (l)(this, m_comm_rank, args...);
    ygm::meta::apply_optional(l, std::make_tuple(this),
                              std::make_tuple(args...));
    return 1;
  }

  template <typename Lambda, typename... PackArgs>
  std::vector<char> pack_lambda(Lambda l, const PackArgs &... args) {
    std::vector<char>             to_return;
    const std::tuple<PackArgs...> tuple_args(
        std::forward<const PackArgs>(args)...);
    ASSERT_DEBUG(sizeof(Lambda) == 1);

    void (*fun_ptr)(impl *, cereal::YGMInputArchive &) =
        [](impl *t, cereal::YGMInputArchive &bia) {
          std::tuple<PackArgs...> ta;
          bia(ta);
          Lambda *pl;
          auto    t1 = std::make_tuple((impl *)t);

          // \pp was: std::apply(*pl, std::tuple_cat(t1, ta));
          ygm::meta::apply_optional(*pl, std::move(t1), std::move(ta));
        };

    cereal::YGMOutputArchive oarchive(to_return);  // Create an output archive
                                                   // // oarchive(fun_ptr);
    int64_t iptr = (int64_t)fun_ptr - (int64_t)&reference;
    oarchive(iptr, tuple_args);

    return to_return;
  }

  // this is used to fix address space randomization
  static void reference() {}

  bool receive_queue_process() {
    bool received = false;
    while (true) {
      auto buffer = receive_queue_try_pop();
      if (buffer == nullptr) break;
      received = true;
      cereal::YGMInputArchive iarchive(buffer->data(), buffer->size());
      while (!iarchive.empty()) {
        int64_t iptr;
        iarchive(iptr);
        iptr += (int64_t)&reference;
        void (*fun_ptr)(impl *, cereal::YGMInputArchive &);
        memcpy(&fun_ptr, &iptr, sizeof(uint64_t));
        fun_ptr(this, iarchive);
        m_recv_count++;
        m_local_rpc_calls++;
      }

      // Only keep buffers of size m_buffer_capacity in pool of buffers
      if (buffer->capacity() == m_buffer_capacity) free_buffer(buffer);
    }
    return received;
  }

  MPI_Comm m_comm_async;
  MPI_Comm m_comm_barrier;
  MPI_Comm m_comm_other;
  int      m_comm_size;
  int      m_comm_rank;
  size_t   m_buffer_capacity;

  std::vector<std::shared_ptr<std::vector<char>>> m_vec_send_buffers;

  std::mutex                                      m_vec_free_buffers_mutex;
  std::vector<std::shared_ptr<std::vector<char>>> m_vec_free_buffers;

  std::deque<std::shared_ptr<std::vector<char>>> m_receive_queue;
  std::mutex                                     m_receive_queue_mutex;

  std::thread m_listener;

  int64_t m_recv_count = 0;
  int64_t m_send_count = 0;

  int64_t m_local_rpc_calls  = 0;
  int64_t m_local_bytes_sent = 0;

  int large_message_announce_tag = 32766;
  int large_message_tag          = 32767;
};

inline comm::comm(int *argc, char ***argv, int buffer_capacity = 16 * 1024) {
  pimpl_if = std::make_shared<detail::mpi_init_finalize>(argc, argv);
  pimpl    = std::make_shared<comm::impl>(MPI_COMM_WORLD, buffer_capacity);
}

inline comm::comm(MPI_Comm mcomm, int buffer_capacity = 16 * 1024) {
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

inline comm::~comm() {
  ASSERT_RELEASE(MPI_Barrier(MPI_COMM_WORLD) == MPI_SUCCESS);
  pimpl.reset();
  ASSERT_RELEASE(MPI_Barrier(MPI_COMM_WORLD) == MPI_SUCCESS);
  pimpl_if.reset();
}

template <typename AsyncFunction, typename... SendArgs>
inline void comm::async(int dest, AsyncFunction fn, const SendArgs &... args) {
  static_assert(std::is_empty<AsyncFunction>::value,
                "Only stateless lambdas are supported");
  pimpl->async(dest, fn, std::forward<const SendArgs>(args)...);
}

template <typename AsyncFunction, typename... SendArgs>
inline void comm::async_bcast(AsyncFunction fn, const SendArgs &... args) {
  static_assert(std::is_empty<AsyncFunction>::value,
                "Only stateless lambdas are supported");
  pimpl->async_bcast(fn, std::forward<const SendArgs>(args)...);
}

template <typename AsyncFunction, typename... SendArgs>
inline void comm::async_mcast(const std::vector<int> &dests, AsyncFunction fn,
                              const SendArgs &... args) {
  static_assert(std::is_empty<AsyncFunction>::value,
                "Only stateless lambdas are supported");
  pimpl->async_mcast(dests, fn, std::forward<const SendArgs>(args)...);
}

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
