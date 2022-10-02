// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once

#include <x86intrin.h>
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
 private:
  friend class ygm::detail::interrupt_mask;

  struct mpi_irecv_request {
    std::shared_ptr<std::byte[]> buffer;
    MPI_Request                  request;
  };

  struct mpi_isend_request {
    std::shared_ptr<std::vector<std::byte>> buffer;
    MPI_Request                             request;
  };

  struct header_t {
    uint16_t message_size;
    uint16_t dest;

    template <typename Archive>
    void serialize(Archive &ar) {
      ar(message_size, dest);
    }
  };

  struct env_configuration {
    env_configuration() {}
    size_t num_irecvs = 64;
  };

  // NR Routing
  int next_hop(const int dest) {
    // if (m_layout.is_local(dest)) {
    //   return dest;
    // } else {
    //   // return m_layout.strided_ranks()[m_layout.node_id(dest)];
    //   const auto [dest_node, dest_local] = m_layout.rank_to_nl(dest);
    //   auto dest_layer_offset             = dest_node % m_layout.local_size();
    //   if (m_layout.local_id() == dest_layer_offset) {
    //     auto my_layer_offset = m_layout.node_id() % m_layout.local_size();
    //     return m_layout.nl_to_rank(dest_node, my_layer_offset);
    //   } else {
    //     return m_layout.nl_to_rank(m_layout.node_id(), dest_layer_offset);
    //   }
    // }
    //
    //  Roger's hack
    //
    // static int tpn       = m_layout.local_size();
    static int my_node   = m_comm_rank / m_layout.local_size();
    static int my_offset = m_comm_rank % m_layout.local_size();
    int        dest_node = dest / m_layout.local_size();
    if (my_node == dest_node) {
      return dest;
    } else {
      //      return dest_node * m_layout.local_size() + my_offset;
      int responsible_core = (dest_node % m_layout.local_size()) +
                             (my_node * m_layout.local_size());
      if (m_comm_rank == responsible_core) {
        return (dest_node * m_layout.local_size()) +
               (my_node % m_layout.local_size());
      } else {
        return responsible_core;
      }
    }
  }

  size_t pack_header(std::vector<std::byte> &packed, const int dest,
                     size_t size) {
    size_t size_before = packed.size();

    header_t h;
    h.dest         = dest;
    h.message_size = size;

    cereal::YGMOutputArchive oarchive(packed);
    oarchive(h);

    return packed.size() - size_before;
  }

 public:
  impl(MPI_Comm c, int buffer_capacity) : m_layout(c) {
    ASSERT_MPI(MPI_Comm_dup(c, &m_comm_async));
    ASSERT_MPI(MPI_Comm_dup(c, &m_comm_barrier));
    ASSERT_MPI(MPI_Comm_dup(c, &m_comm_other));
    ASSERT_MPI(MPI_Comm_size(m_comm_async, &m_comm_size));
    ASSERT_MPI(MPI_Comm_rank(m_comm_async, &m_comm_rank));
    m_buffer_capacity_bytes = buffer_capacity;

    m_vec_send_buffers.resize(m_comm_size);

    if (m_comm_rank == 0) {
      std::cout << "config.num_irecvs = " << config.num_irecvs << std::endl;
    }
    for (size_t i = 0; i < config.num_irecvs; ++i) {
      std::shared_ptr<std::byte[]> recv_buffer{
          new std::byte[m_buffer_capacity_bytes / m_layout.local_size()]};
      post_new_irecv(recv_buffer);
    }

    m_packing_buffer.reserve(1024);
  }

  ~impl() {
    ASSERT_RELEASE(MPI_Barrier(m_comm_async) == MPI_SUCCESS);
    if (m_comm_rank == 0) {
      std::cout << "m_local_isend = " << m_local_isend << std::endl;
      std::cout << "m_local_isend_test = " << m_local_isend_test << std::endl;
      std::cout << "m_local_irecv = " << m_local_irecv << std::endl;
      std::cout << "m_local_irecv_test = " << m_local_irecv_test << std::endl;
      std::cout << "m_local_iallreduce_test = " << m_local_iallreduce_test
                << std::endl;
      std::cout << "m_local_iallreduce = " << m_local_iallreduce << std::endl;
      std::cout << "m_mpi_wait_time = " << m_mpi_wait_time << std::endl;
      std::cout << "m_enable_routing = " << m_enable_routing << std::endl;
      std::cout << "m_buffer_capacity_bytes = " << m_buffer_capacity_bytes
                << std::endl;
    }

    ASSERT_RELEASE(m_send_queue.empty());
    ASSERT_RELEASE(m_send_dest_queue.empty());
    ASSERT_RELEASE(m_send_buffer_bytes == 0);
    ASSERT_RELEASE(m_isend_bytes == 0);

    for (size_t i = 0; i < m_recv_queue.size(); ++i) {
      ASSERT_RELEASE(MPI_Cancel(&(m_recv_queue[i].request)) == MPI_SUCCESS);
    }
    ASSERT_RELEASE(MPI_Barrier(m_comm_async) == MPI_SUCCESS);
    if (m_comm_rank == 0) {
      std::cout << "Last barrier" << std::endl;
    }
    ASSERT_RELEASE(MPI_Comm_free(&m_comm_async) == MPI_SUCCESS);
    ASSERT_RELEASE(MPI_Comm_free(&m_comm_barrier) == MPI_SUCCESS);
    ASSERT_RELEASE(MPI_Comm_free(&m_comm_other) == MPI_SUCCESS);
    if (m_comm_rank == 0) {
      std::cout << "Last MPI_Comm_free" << std::endl;
    }
  }

  int size() const { return m_comm_size; }
  int rank() const { return m_comm_rank; }

  template <typename... SendArgs>
  void async(int dest, const SendArgs &...args) {
    ASSERT_RELEASE(dest < m_comm_size);

    // check_if_production_halt_required();

    m_send_count++;

    //
    //
    int next_dest = dest;
    if (m_enable_routing) {
      next_hop(dest);
    }

    //
    // add data to the to dest buffer
    if (m_vec_send_buffers[next_dest].empty()) {
      m_send_dest_queue.push_back(next_dest);
      m_vec_send_buffers[next_dest].reserve(m_buffer_capacity_bytes /
                                            m_layout.local_size());
    }

    // // Add header without message size
    size_t header_bytes = 0;
    if (m_enable_routing) {
      header_bytes = pack_header(m_vec_send_buffers[next_dest], dest, 0);
      m_local_bytes_sent += header_bytes;
      m_send_buffer_bytes += header_bytes;
    }

    size_t bytes = pack_lambda(m_vec_send_buffers[next_dest],
                               std::forward<const SendArgs>(args)...);
    m_local_bytes_sent += bytes;
    m_send_buffer_bytes += bytes;

    // // Add message size to header
    if (m_enable_routing) {
      auto iter = m_vec_send_buffers[next_dest].end();
      iter -= (header_bytes + bytes);
      std::memcpy(&*iter, &bytes, sizeof(header_t::dest));
    }

    //
    // Check if send buffer capacity has been exceeded
    if (!in_process_receive_queue) {
      flush_to_capacity();
    }
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
      if (current_counts.first != current_counts.second) {
        flush_all_local_and_process_incoming();
      }
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

    ASSERT_RELEASE(m_isend_bytes == 0);
    ASSERT_RELEASE(m_send_buffer_bytes == 0);

    MPI_Request req = MPI_REQUEST_NULL;
    ASSERT_MPI(MPI_Iallreduce(local_counts, global_counts, 2, MPI_UINT64_T,
                              MPI_SUM, m_comm_barrier, &req));
    m_local_iallreduce++;
    bool iallreduce_complete(false);
    while (!iallreduce_complete) {
      MPI_Request twin_req[2];
      twin_req[0] = req;
      twin_req[1] = m_recv_queue.front().request;

      int        outcount;
      int        twin_indices[2];
      MPI_Status twin_status[2];
      double     start_wait = MPI_Wtime();
      ASSERT_MPI(
          MPI_Waitsome(2, twin_req, &outcount, twin_indices, twin_status));
      m_mpi_wait_time += MPI_Wtime() - start_wait;
      for (int i = 0; i < outcount; ++i) {
        if (twin_indices[i] == 0) {  // completed a Iallreduce
          iallreduce_complete = true;
          // std::cout << m_comm_rank << ": iallreduce_complete: " <<
          // global_counts[0] << " " << global_counts[1] << std::endl;
        } else {
          handle_next_receive(twin_status[i]);
          flush_all_local_and_process_incoming();
        }
      }
    }
    return {global_counts[0], global_counts[1]};
  }

  /**
   * @brief Flushes send buffer to dest
   *
   * @param dest
   */
  void flush_send_buffer(int dest) {
    if (m_vec_send_buffers[dest].size() > 0) {
      mpi_isend_request request;
      if (m_free_send_buffers.empty()) {
        request.buffer = std::make_shared<std::vector<std::byte>>();
      } else {
        request.buffer = m_free_send_buffers.back();
        m_free_send_buffers.pop_back();
      }
      request.buffer->swap(m_vec_send_buffers[dest]);

      ASSERT_MPI(MPI_Isend(request.buffer->data(), request.buffer->size(),
                           MPI_BYTE, dest, 0, m_comm_async,
                           &(request.request)));
      m_local_isend++;
      m_isend_bytes += request.buffer->size();
      m_send_buffer_bytes -= request.buffer->size();
      m_send_queue.push_back(request);
      if (!in_process_receive_queue) {
        process_receive_queue();
      }
    }
  }

  void check_if_production_halt_required() {
    while (m_enable_interrupts && !in_process_receive_queue &&
           m_isend_bytes > m_buffer_capacity_bytes) {
      process_receive_queue();
    }
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

      //
      // Wait on isends
      while (!m_send_queue.empty()) {
        did_something |= process_receive_queue();
      }
    }
  }

  /**
   * @brief Flush send buffers until queued sends are smaller than buffer
   * capacity
   */
  void flush_to_capacity() {
    while (m_send_buffer_bytes > m_buffer_capacity_bytes) {
      ASSERT_DEBUG(!m_send_dest_queue.empty());
      int dest = m_send_dest_queue.front();
      m_send_dest_queue.pop_front();
      flush_send_buffer(dest);
    }
  }

  void post_new_irecv(std::shared_ptr<std::byte[]> &recv_buffer) {
    mpi_irecv_request recv_req;
    recv_req.buffer = recv_buffer;

    ASSERT_MPI(MPI_Irecv(recv_req.buffer.get(),
                         m_buffer_capacity_bytes / m_layout.local_size(),
                         MPI_BYTE, MPI_ANY_SOURCE, MPI_ANY_TAG, m_comm_async,
                         &(recv_req.request)));
    m_local_irecv++;
    m_recv_queue.push_back(recv_req);
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
          Lambda *pl = nullptr;
          size_t  l_storage[sizeof(Lambda) / sizeof(size_t) +
                           (sizeof(Lambda) % sizeof(size_t) > 0)];
          if constexpr (!std::is_empty<Lambda>::value) {
            bia.loadBinary(l_storage, sizeof(Lambda));
            pl = (Lambda *)l_storage;
          }

          std::tuple<PackArgs...> ta;
          if constexpr (!std::is_empty<std::tuple<PackArgs...>>::value) {
            bia(ta);
          }

          auto t1 = std::make_tuple((comm *)c);

          // \pp was: std::apply(*pl, std::tuple_cat(t1, ta));
          ygm::meta::apply_optional(*pl, std::move(t1), std::move(ta));
        };

    // // oarchive(fun_ptr);
    int64_t iptr = (int64_t)fun_ptr - (int64_t)&reference;
    // ptrdiff_t iptr = (void *)fun_ptr - reinterpret_cast<void *>(&reference);
    ASSERT_RELEASE(iptr < std::numeric_limits<int32_t>::max() &&
                   iptr > std::numeric_limits<int32_t>::min());
    int32_t iptr_32 = iptr;

    {
      // oarchive(iptr_32);
      size_t size_before = packed.size();
      packed.resize(size_before + sizeof(iptr_32));
      std::memcpy(packed.data() + size_before, &iptr_32, sizeof(iptr_32));
    }

    if constexpr (!std::is_empty<Lambda>::value) {
      // oarchive.saveBinary(&l, sizeof(Lambda));
      size_t size_before = packed.size();
      packed.resize(size_before + sizeof(Lambda));
      std::memcpy(packed.data() + size_before, &l, sizeof(Lambda));
    }

    if constexpr (!std::is_empty<std::tuple<PackArgs...>>::value) {
      // Only create cereal archive is tuple needs serialization
      cereal::YGMOutputArchive oarchive(packed);  // Create an output archive
      oarchive(tuple_args);
    }
    return packed.size() - size_before;
  }

  /**
   * @brief Static reference point to anchor address space randomization.
   *
   */
  static void reference() {}

  void handle_next_receive(MPI_Status status) {
    comm tmp_comm(shared_from_this());
    int  count{0};
    ASSERT_MPI(MPI_Get_count(&status, MPI_BYTE, &count));
    // std::cout << m_comm_rank << ": received " << count << std::endl;
    cereal::YGMInputArchive iarchive(m_recv_queue.front().buffer.get(), count);
    while (!iarchive.empty()) {
      if (m_enable_routing) {
        header_t h;
        iarchive(h);
        if (h.dest == m_comm_rank) {
          int32_t iptr_32;
          iarchive(iptr_32);
          uint64_t iptr = iptr_32;
          iptr += (int64_t)&reference;
          void (*fun_ptr)(comm *, cereal::YGMInputArchive &);
          std::memcpy(&fun_ptr, &iptr, sizeof(iptr));
          fun_ptr(&tmp_comm, iarchive);
          m_recv_count++;
          m_local_rpc_calls++;
        } else {
          int next_dest = next_hop(h.dest);

          if (m_vec_send_buffers[next_dest].empty()) {
            m_send_dest_queue.push_back(next_dest);
          }

          size_t header_bytes = pack_header(m_vec_send_buffers[next_dest],
                                            h.dest, h.message_size);
          m_local_bytes_sent += header_bytes;
          m_send_buffer_bytes += header_bytes;

          size_t precopy_size = m_vec_send_buffers[next_dest].size();
          m_vec_send_buffers[next_dest].resize(precopy_size + h.message_size);
          iarchive.loadBinary(&m_vec_send_buffers[next_dest][precopy_size],
                              h.message_size);

          m_local_bytes_sent += h.message_size;
          m_send_buffer_bytes += h.message_size;

          flush_to_capacity();
        }
      } else {
        int32_t iptr_32;
        iarchive(iptr_32);
        uint64_t iptr = iptr_32;
        iptr += (int64_t)&reference;
        void (*fun_ptr)(comm *, cereal::YGMInputArchive &);
        std::memcpy(&fun_ptr, &iptr, sizeof(iptr));
        fun_ptr(&tmp_comm, iarchive);
        m_recv_count++;
        m_local_rpc_calls++;
      }
      // //////////header_t h;
      // iarchive(h);
      // if (h.dest == m_comm_rank) {
      // int64_t iptr;
      // iarchive(iptr);
      // iptr += (int64_t)&reference;
      // void (*fun_ptr)(comm *, cereal::YGMInputArchive &);
      // memcpy(&fun_ptr, &iptr, sizeof(uint64_t));
      // fun_ptr(&tmp_comm, iarchive);
      // m_recv_count++;
      // m_local_rpc_calls++;
      // } else {
      //   int next_dest = next_hop(h.dest);

      //   if (m_vec_send_buffers[next_dest].empty()) {
      //     m_send_dest_queue.push_back(next_dest);
      //   }

      //   size_t header_bytes = pack_header(m_vec_send_buffers[next_dest],
      //                                     h.dest, h.message_size);
      //   m_local_bytes_sent += header_bytes;
      //   m_send_buffer_bytes += header_bytes;

      //   size_t precopy_size = m_vec_send_buffers[next_dest].size();
      //   m_vec_send_buffers[next_dest].resize(precopy_size + h.message_size);
      //   iarchive.loadBinary(&m_vec_send_buffers[next_dest][precopy_size],
      //                       h.message_size);

      //   m_local_bytes_sent += h.message_size;
      //   m_send_buffer_bytes += h.message_size;
      // }
    }
    post_new_irecv(m_recv_queue.front().buffer);
    m_recv_queue.pop_front();
    flush_to_capacity();
  }

  /**
   * @brief Process receive queue of messages received by the listener thread.
   *
   * @return True if receive queue was non-empty, else false
   */
  bool process_receive_queue() {
    ASSERT_RELEASE(!in_process_receive_queue);
    in_process_receive_queue = true;
    bool received_to_return  = false;

    if (!m_enable_interrupts) {
      in_process_receive_queue = false;
      return received_to_return;
    }

    //
    // if we have a pending iRecv, then we can issue a Waitsome
    if (m_send_queue.size() > config.num_irecvs) {
      MPI_Request twin_req[2];
      twin_req[0] = m_send_queue.front().request;
      twin_req[1] = m_recv_queue.front().request;

      int        outcount;
      int        twin_indices[2];
      MPI_Status twin_status[2];
      double     start_wait = MPI_Wtime();
      ASSERT_MPI(
          MPI_Waitsome(2, twin_req, &outcount, twin_indices, twin_status));
      m_mpi_wait_time += MPI_Wtime() - start_wait;
      for (int i = 0; i < outcount; ++i) {
        if (twin_indices[i] == 0) {  // completed a iSend
          m_isend_bytes -= m_send_queue.front().buffer->size();
          m_send_queue.front().buffer->clear();
          m_free_send_buffers.push_back(m_send_queue.front().buffer);
          m_send_queue.pop_front();
        } else {  // completed an iRecv -- COPIED FROM BELOW
          received_to_return = true;
          handle_next_receive(twin_status[i]);
        }
      }
    } else {
      if (!m_send_queue.empty()) {
        int flag(0);
        ASSERT_MPI(MPI_Test(&(m_send_queue.front().request), &flag,
                            MPI_STATUS_IGNORE));
        m_local_isend_test++;
        if (flag) {
          m_isend_bytes -= m_send_queue.front().buffer->size();
          m_send_queue.front().buffer->clear();
          m_free_send_buffers.push_back(m_send_queue.front().buffer);
          m_send_queue.pop_front();
        }
      }
    }

    while (true) {
      int        flag(0);
      MPI_Status status;
      ASSERT_MPI(MPI_Test(&(m_recv_queue.front().request), &flag, &status));
      m_local_irecv_test++;
      if (flag) {
        received_to_return = true;
        handle_next_receive(status);
      } else {
        break;  // not ready yet
      }
    }

    in_process_receive_queue = false;
    return received_to_return;
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

  std::deque<mpi_irecv_request>                        m_recv_queue;
  std::deque<mpi_isend_request>                        m_send_queue;
  std::vector<std::shared_ptr<std::vector<std::byte>>> m_free_send_buffers;

  // size_t     m_recv_queue_bytes = 0;
  size_t m_isend_bytes = 0;

  std::deque<std::function<void()>> m_pre_barrier_callbacks;

  bool m_enable_interrupts = true;

  uint64_t m_recv_count = 0;
  uint64_t m_send_count = 0;

  int64_t m_local_rpc_calls  = 0;
  int64_t m_local_bytes_sent = 0;

  bool in_process_receive_queue = false;

  size_t m_local_isend           = 0;
  size_t m_local_isend_test      = 0;
  size_t m_local_irecv           = 0;
  size_t m_local_irecv_test      = 0;
  size_t m_local_iallreduce_test = 0;
  size_t m_local_iallreduce      = 0;
  double m_mpi_wait_time         = 0;

  bool m_enable_routing = 0;

  const env_configuration config;  // todo: make const?

  std::vector<std::byte> m_packing_buffer;
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
  static_assert(std::is_trivially_copyable<AsyncFunction>::value &&
                    std::is_standard_layout<AsyncFunction>::value,
                "comm::async() AsyncFunction must be is_trivially_copyable & "
                "is_standard_layout.");
  pimpl->async(dest, fn, std::forward<const SendArgs>(args)...);
}

template <typename AsyncFunction, typename... SendArgs>
inline void comm::async_bcast(AsyncFunction fn, const SendArgs &...args) {
  static_assert(
      std::is_trivially_copyable<AsyncFunction>::value &&
          std::is_standard_layout<AsyncFunction>::value,
      "comm::async_bcast() AsyncFunction must be is_trivially_copyable & "
      "is_standard_layout.");
  pimpl->async_bcast(fn, std::forward<const SendArgs>(args)...);
}

template <typename AsyncFunction, typename... SendArgs>
inline void comm::async_mcast(const std::vector<int> &dests, AsyncFunction fn,
                              const SendArgs &...args) {
  static_assert(
      std::is_trivially_copyable<AsyncFunction>::value &&
          std::is_standard_layout<AsyncFunction>::value,
      "comm::async_mcast() AsyncFunction must be is_trivially_copyable & "
      "is_standard_layout.");
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
