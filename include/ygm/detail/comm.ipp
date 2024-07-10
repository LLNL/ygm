// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once
#include <iomanip>
#include <ygm/detail/meta/functional.hpp>
#include <ygm/detail/ygm_cereal_archive.hpp>

namespace ygm {

struct comm::mpi_irecv_request {
  std::shared_ptr<std::byte[]> buffer;
  MPI_Request                  request;
};

struct comm::mpi_isend_request {
  std::shared_ptr<std::vector<std::byte>> buffer;
  MPI_Request                             request;
  int32_t                                 id;
};

struct comm::header_t {
  uint32_t message_size;
  int32_t  dest;
};

struct comm::trace_header_t {
  int32_t from;
  int32_t trace_id;
};

inline comm::comm(int *argc, char ***argv)
    : pimpl_if(std::make_shared<detail::mpi_init_finalize>(argc, argv)),
      m_layout(MPI_COMM_WORLD),
      m_router(m_layout, config.routing) {
  // pimpl_if = std::make_shared<detail::mpi_init_finalize>(argc, argv);
  comm_setup(MPI_COMM_WORLD);
}

inline comm::comm(MPI_Comm mcomm)
    : m_layout(mcomm), m_router(m_layout, config.routing) {
  pimpl_if.reset();
  int flag(0);
  ASSERT_MPI(MPI_Initialized(&flag));
  if (!flag) {
    throw std::runtime_error("YGM::COMM ERROR: MPI not initialized");
  }
  comm_setup(mcomm);
}

inline void comm::comm_setup(MPI_Comm c) {
  ASSERT_MPI(MPI_Comm_dup(c, &m_comm_async));
  ASSERT_MPI(MPI_Comm_dup(c, &m_comm_barrier));
  ASSERT_MPI(MPI_Comm_dup(c, &m_comm_other));

  m_vec_send_buffers.resize(m_layout.size());

  if (config.welcome) {
    welcome(std::cout);
  }

  for (size_t i = 0; i < config.num_irecvs; ++i) {
    std::shared_ptr<std::byte[]> recv_buffer{new std::byte[config.irecv_size]};
    post_new_irecv(recv_buffer);
  }

  if (config.trace_ygm || config.trace_mpi) {
    if (rank0()) m_tracer.create_directory(config.trace_path);
    ASSERT_MPI(MPI_Barrier(c));
    m_tracer.open_file(config.trace_path, rank());
    m_next_message_id = rank();
  }
}

inline void comm::welcome(std::ostream &os) {
  static bool already_printed = false;
  if (already_printed) return;
  already_printed = true;
  std::stringstream sstr;
  sstr << "======================================\n"
       << " YY    YY     GGGGGG      MM     MM   \n"
       << "  YY  YY     GG    GG     MMM   MMM   \n"
       << "   YYYY      GG           MMMM MMMM   \n"
       << "    YY       GG   GGGG    MM MMM MM   \n"
       << "    YY       GG    GG     MM     MM   \n"
       << "    YY       GG    GG     MM     MM   \n"
       << "    YY        GGGGGG      MM     MM   \n"
       << "======================================\n"
       << "COMM_SIZE      = " << m_layout.size() << "\n"
       << "RANKS_PER_NODE = " << m_layout.local_size() << "\n"
       << "NUM_NODES      = " << m_layout.node_size() << "\n";

  config.print(sstr);

  if (rank() == 0) {
    os << sstr.str();
  }
}

inline void comm::stats_reset() { stats.reset(); }
inline void comm::stats_print(const std::string &name, std::ostream &os) {
  std::stringstream sstr;
  sstr << "============== STATS =================\n"
       << "NAME                     = " << name << "\n"
       << "TIME                     = " << stats.get_elapsed_time() << "\n"
       << "GLOBAL_ASYNC_COUNT       = "
       << all_reduce_sum(stats.get_async_count()) << "\n"
       << "GLOBAL_ISEND_COUNT       = "
       << all_reduce_sum(stats.get_isend_count()) << "\n"
       << "GLOBAL_ISEND_BYTES       = "
       << all_reduce_sum(stats.get_isend_bytes()) << "\n"
       << "MAX_WAITSOME_ISEND_IRECV = "
       << all_reduce_max(stats.get_waitsome_isend_irecv_time()) << "\n"
       << "MAX_WAITSOME_IALLREDUCE  = "
       << all_reduce_max(stats.get_waitsome_iallreduce_time()) << "\n"
       << "COUNT_IALLREDUCE         = " << stats.get_iallreduce_count() << "\n"
       << "======================================";

  if (rank0()) {
    os << sstr.str() << std::endl;
  }
}

inline comm::~comm() {
  barrier();

  for (int i = 0; i < m_layout.size(); i++) {
    if (rank() == i) {
      std::cout << "Rank " << rank() << std::setw(30)
                << "send_buffer_count = " << send_buffer_count << std::setw(30)
                << "receive_buffer_count = " << receive_buffer_count
                << std::setw(30)
                << "receive_queue_completed = " << receive_queue_completed
                << std::setw(30)
                << "send_queue_completed = " << send_queue_completed
                << std::endl;
      // std::cout << "request = " << request_count << std::endl;
    }
  }

  ASSERT_RELEASE(MPI_Barrier(m_comm_async) == MPI_SUCCESS);

  ASSERT_RELEASE(m_send_queue.empty());
  ASSERT_RELEASE(m_send_dest_queue.empty());
  ASSERT_RELEASE(m_send_buffer_bytes == 0);
  ASSERT_RELEASE(m_pending_isend_bytes == 0);

  for (size_t i = 0; i < m_recv_queue.size(); ++i) {
    ASSERT_RELEASE(MPI_Cancel(&(m_recv_queue[i].request)) == MPI_SUCCESS);
  }
  ASSERT_RELEASE(MPI_Barrier(m_comm_async) == MPI_SUCCESS);
  ASSERT_RELEASE(MPI_Comm_free(&m_comm_async) == MPI_SUCCESS);
  ASSERT_RELEASE(MPI_Comm_free(&m_comm_barrier) == MPI_SUCCESS);
  ASSERT_RELEASE(MPI_Comm_free(&m_comm_other) == MPI_SUCCESS);

  pimpl_if.reset();
}

template <typename AsyncFunction, typename... SendArgs>
inline void comm::async(int dest, AsyncFunction fn, const SendArgs &...args) {
  TimeResolution event_time;
  if (config.trace_ygm) event_time = m_tracer.get_time();

  static_assert(std::is_trivially_copyable<AsyncFunction>::value &&
                    std::is_standard_layout<AsyncFunction>::value,
                "comm::async() AsyncFunction must be is_trivially_copyable & "
                "is_standard_layout.");
  ASSERT_RELEASE(dest < m_layout.size());
  stats.async(dest);

  check_if_production_halt_required();
  m_send_count++;

  //
  //
  int next_dest = dest;
  if (config.routing != detail::routing_type::NONE) {
    // next_dest = next_hop(dest);
    next_dest = m_router.next_hop(dest);
  }

  //
  // add data to the to dest buffer
  if (m_vec_send_buffers[next_dest].empty()) {
    m_send_dest_queue.push_back(next_dest);
    m_vec_send_buffers[next_dest].reserve(config.buffer_size /
                                          m_layout.node_size());
  }

  // // Add header without message size
  size_t header_bytes = 0;
  if (config.routing != detail::routing_type::NONE) {
    header_bytes = pack_routing_header(m_vec_send_buffers[next_dest], dest, 0);
    m_send_buffer_bytes += header_bytes;
  }

  // TODO: Add tracing header
  size_t trace_header_bytes = 0;
  if (config.trace_ygm) {
    m_next_message_id += size();
    trace_header_bytes = pack_tracing_header(m_vec_send_buffers[next_dest],
                                             m_next_message_id, 0);
    m_send_buffer_bytes += trace_header_bytes;
  }

  uint32_t bytes = pack_lambda(m_vec_send_buffers[next_dest], fn,
                               std::forward<const SendArgs>(args)...);
  m_send_buffer_bytes += bytes;

  // // Add message size to header
  if (config.routing != detail::routing_type::NONE) {
    auto iter = m_vec_send_buffers[next_dest].end();
    iter -= (header_bytes + bytes);
    if (config.trace_ygm) iter -= trace_header_bytes;

    std::memcpy(&*iter, &bytes,
                sizeof(header_t::dest));  // TODO:: TYPO? should it be
                                          // header_t::message_size
  }
  //
  // Check if send buffer capacity has been exceeded
  if (!m_in_process_receive_queue) {
    flush_to_capacity();
  }

  if (config.trace_ygm) {
    TimeResolution duration = m_tracer.get_time() - event_time;

    std::unordered_map<std::string, std::any> metadata;
    metadata["from"]         = rank();
    metadata["to"]           = dest;
    metadata["event_id"]     = m_next_message_id;
    metadata["message_size"] = bytes;

    ConstEventType event_name = "async";
    ConstEventType action     = "send";

    m_tracer.trace_event(m_next_message_id, action, event_name, rank(),
                         event_time, metadata, 'X', duration);
  }
}

template <typename AsyncFunction, typename... SendArgs>
inline void comm::async_bcast(AsyncFunction fn, const SendArgs &...args) {
  static_assert(
      std::is_trivially_copyable<AsyncFunction>::value &&
          std::is_standard_layout<AsyncFunction>::value,
      "comm::async_bcast() AsyncFunction must be is_trivially_copyable & "
      "is_standard_layout.");
  check_if_production_halt_required();

  pack_lambda_broadcast(fn, std::forward<const SendArgs>(args)...);

  //
  // Check if send buffer capacity has been exceeded
  if (!m_in_process_receive_queue) {
    flush_to_capacity();
  }
}

template <typename AsyncFunction, typename... SendArgs>
inline void comm::async_mcast(const std::vector<int> &dests, AsyncFunction fn,
                              const SendArgs &...args) {
  static_assert(
      std::is_trivially_copyable<AsyncFunction>::value &&
          std::is_standard_layout<AsyncFunction>::value,
      "comm::async_mcast() AsyncFunction must be is_trivially_copyable & "
      "is_standard_layout.");
  for (auto dest : dests) {
    async(dest, fn, std::forward<const SendArgs>(args)...);
  }
}

inline const detail::layout &comm::layout() const { return m_layout; }

inline const detail::comm_router &comm::router() const { return m_router; }

inline int comm::size() const {
  return m_layout.size();
  ;
}
inline int comm::rank() const { return m_layout.rank(); }

inline MPI_Comm comm::get_mpi_comm() const { return m_comm_other; }

/**
 * @brief Full communicator barrier
 *
 */
inline void comm::barrier() {
  TimeResolution start_time;
  if (config.trace_ygm) {
    start_time = m_tracer.get_time();
  }

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

  if (config.trace_ygm) {
    m_next_message_id += size();
    std::unordered_map<std::string, std::any> metadata;
    metadata["m_pending_isend_bytes"] = m_pending_isend_bytes;
    metadata["m_send_buffer_bytes"]   = m_send_buffer_bytes;
    metadata["m_recv_count"]          = m_recv_count;
    metadata["m_send_count"]          = m_send_count;
    ConstEventType event_name         = "barrier";
    ConstEventType action             = "barrier";
    TimeResolution duration           = m_tracer.get_time() - start_time;

    m_tracer.trace_event(m_next_message_id, action, event_name, rank(),
                         start_time, metadata, 'X', duration);
  }
}

/**
 * @brief Control Flow Barrier
 * Only blocks the control flow until all processes in the communicator have
 * called it. See:  MPI_Barrier()
 */
inline void comm::cf_barrier() { ASSERT_MPI(MPI_Barrier(m_comm_barrier)); }

template <typename T>
inline ygm_ptr<T> comm::make_ygm_ptr(T &t) {
  ygm_ptr<T> to_return(&t);
  to_return.check(*this);
  return to_return;
}

inline void comm::register_pre_barrier_callback(
    const std::function<void()> &fn) {
  m_pre_barrier_callbacks.push_back(fn);
}

template <typename T>
inline T comm::all_reduce_sum(const T &t) const {
  T to_return;
  ASSERT_MPI(MPI_Allreduce(&t, &to_return, 1, detail::mpi_typeof(T()), MPI_SUM,
                           m_comm_other));
  return to_return;
}

template <typename T>
inline T comm::all_reduce_min(const T &t) const {
  T to_return;
  ASSERT_MPI(MPI_Allreduce(&t, &to_return, 1, detail::mpi_typeof(T()), MPI_MIN,
                           m_comm_other));
  return to_return;
}

template <typename T>
inline T comm::all_reduce_max(const T &t) const {
  T to_return;
  ASSERT_MPI(MPI_Allreduce(&t, &to_return, 1, detail::mpi_typeof(T()), MPI_MAX,
                           m_comm_other));
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
inline T comm::all_reduce(const T &in, MergeFunction merge) const {
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

template <typename T>
inline void comm::mpi_send(const T &data, int dest, int tag,
                           MPI_Comm comm) const {
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
inline T comm::mpi_recv(int source, int tag, MPI_Comm comm) const {
  std::vector<std::byte> packed;
  size_t                 packed_size{0};
  ASSERT_MPI(MPI_Recv(&packed_size, 1, detail::mpi_typeof(packed_size), source,
                      tag, comm, MPI_STATUS_IGNORE));
  packed.resize(packed_size);
  ASSERT_MPI(MPI_Recv(packed.data(), packed_size, MPI_BYTE, source, tag, comm,
                      MPI_STATUS_IGNORE));

  T                       to_return;
  cereal::YGMInputArchive iarchive(packed.data(), packed.size());
  iarchive(to_return);
  return to_return;
}

template <typename T>
inline T comm::mpi_bcast(const T &to_bcast, int root, MPI_Comm comm) const {
  std::vector<std::byte>   packed;
  cereal::YGMOutputArchive oarchive(packed);
  if (rank() == root) {
    oarchive(to_bcast);
  }
  size_t packed_size = packed.size();
  ASSERT_RELEASE(packed_size < 1024 * 1024 * 1024);
  ASSERT_MPI(
      MPI_Bcast(&packed_size, 1, detail::mpi_typeof(packed_size), root, comm));
  if (rank() != root) {
    packed.resize(packed_size);
  }
  ASSERT_MPI(MPI_Bcast(packed.data(), packed_size, MPI_BYTE, root, comm));

  cereal::YGMInputArchive iarchive(packed.data(), packed.size());
  T                       to_return;
  iarchive(to_return);
  return to_return;
}

inline std::ostream &comm::cout0() const {
  static std::ostringstream dummy;
  dummy.clear();
  if (rank() == 0) {
    return std::cout;
  }
  return dummy;
}

inline std::ostream &comm::cerr0() const {
  static std::ostringstream dummy;
  dummy.clear();
  if (rank() == 0) {
    return std::cerr;
  }
  return dummy;
}

inline std::ostream &comm::cout() const {
  std::cout << rank() << ": ";
  return std::cout;
}

inline std::ostream &comm::cerr() const {
  std::cerr << rank() << ": ";
  return std::cerr;
}

template <typename... Args>
inline void comm::cout(Args &&...args) const {
  std::cout << outstr(args...) << std::endl;
}

template <typename... Args>
inline void comm::cerr(Args &&...args) const {
  std::cerr << outstr(args...) << std::endl;
}

template <typename... Args>
inline void comm::cout0(Args &&...args) const {
  if (rank0()) {
    std::cout << outstr0(args...) << std::endl;
  }
}

template <typename... Args>
inline void comm::cerr0(Args &&...args) const {
  if (rank0()) {
    std::cerr << outstr0(args...) << std::endl;
  }
}

template <typename... Args>
inline std::string comm::outstr0(Args &&...args) const {
  std::stringstream ss;
  (ss << ... << args);
  return ss.str();
}

template <typename... Args>
inline std::string comm::outstr(Args &&...args) const {
  std::stringstream ss;
  (ss << rank() << ": " << ... << args);
  return ss.str();
}

inline size_t comm::pack_routing_header(std::vector<std::byte> &packed,
                                        const int dest, size_t size) {
  size_t size_before = packed.size();

  header_t h;
  h.dest         = dest;
  h.message_size = size;

  packed.resize(size_before + sizeof(header_t));
  std::memcpy(packed.data() + size_before, &h, sizeof(header_t));

  // cereal::YGMOutputArchive oarchive(packed);
  // oarchive(h);

  return packed.size() - size_before;
}

inline size_t comm::pack_tracing_header(std::vector<std::byte> &packed,
                                        const int trace_id, size_t size) {
  size_t size_before = packed.size();

  trace_header_t h;
  h.from     = rank();
  h.trace_id = trace_id;

  packed.resize(size_before + sizeof(trace_header_t));
  std::memcpy(packed.data() + size_before, &h, sizeof(trace_header_t));

  return packed.size() - size_before;
}

inline std::pair<uint64_t, uint64_t> comm::barrier_reduce_counts() {
  uint64_t local_counts[2]  = {m_recv_count, m_send_count};
  uint64_t global_counts[2] = {0, 0};

  ASSERT_RELEASE(m_pending_isend_bytes == 0);
  ASSERT_RELEASE(m_send_buffer_bytes == 0);

  MPI_Request req = MPI_REQUEST_NULL;
  ASSERT_MPI(MPI_Iallreduce(local_counts, global_counts, 2, MPI_UINT64_T,
                            MPI_SUM, m_comm_barrier, &req));
  stats.iallreduce();
  bool iallreduce_complete(false);
  while (!iallreduce_complete) {
    MPI_Request twin_req[2];
    twin_req[0] = req;
    twin_req[1] = m_recv_queue.front().request;

    int        outcount{0};
    int        twin_indices[2];
    MPI_Status twin_status[2];

    {
      auto timer = stats.waitsome_iallreduce();
      while (outcount == 0) {
        ASSERT_MPI(
            MPI_Testsome(2, twin_req, &outcount, twin_indices, twin_status));
      }
    }

    for (int i = 0; i < outcount; ++i) {
      if (twin_indices[i] == 0) {  // completed a Iallreduce
        iallreduce_complete = true;
        // std::cout << m_layout.rank() << ": iallreduce_complete: " <<
        // global_counts[0] << " " << global_counts[1] << std::endl;
      } else {
        receive_buffer_count++;
        if (config.trace_mpi) {
          TimeResolution event_time = m_tracer.get_time();
          std::unordered_map<std::string, std::any> metadata;
          metadata["type"] = "barrier_reduce_counts";

          ConstEventType event_name = "mpi";
          ConstEventType action     = "mpi_receive";

          m_tracer.trace_event(0, action, event_name, rank(), event_time,
                               metadata);
        }
        mpi_irecv_request req_buffer = m_recv_queue.front();
        m_recv_queue.pop_front();
        int buffer_size{0};
        ASSERT_MPI(MPI_Get_count(&twin_status[i], MPI_BYTE, &buffer_size));
        stats.irecv(twin_status[i].MPI_SOURCE, buffer_size);
        handle_next_receive(req_buffer.buffer, buffer_size);
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
inline void comm::flush_send_buffer(int dest) {
  static size_t counter = 0;
  if (m_vec_send_buffers[dest].size() > 0) {
    mpi_isend_request request;

    m_next_message_id += size();
    request.id = m_next_message_id;

    if (m_free_send_buffers.empty()) {
      request.buffer = std::make_shared<std::vector<std::byte>>();
    } else {
      request.buffer = m_free_send_buffers.back();
      m_free_send_buffers.pop_back();
    }
    request.buffer->swap(m_vec_send_buffers[dest]);
    if (config.freq_issend > 0 && counter++ % config.freq_issend == 0) {
      ASSERT_MPI(MPI_Issend(request.buffer->data(), request.buffer->size(),
                            MPI_BYTE, dest, 0, m_comm_async,
                            &(request.request)));
    } else {
      ASSERT_MPI(MPI_Isend(request.buffer->data(), request.buffer->size(),
                           MPI_BYTE, dest, 0, m_comm_async,
                           &(request.request)));
    }

    send_buffer_count++;
    if (config.trace_mpi) {
      TimeResolution event_time = m_tracer.get_time();
      std::unordered_map<std::string, std::any> metadata;
      metadata["type"] = "mpi_send";

      ConstEventType event_name = "mpi";
      ConstEventType action     = "mpi_send";

      m_tracer.trace_event(m_next_message_id, action, event_name, rank(),
                           event_time, metadata, 'b');
    }

    stats.isend(dest, request.buffer->size());
    m_pending_isend_bytes += request.buffer->size();
    m_send_buffer_bytes -= request.buffer->size();
    m_send_queue.push_back(request);
    if (!m_in_process_receive_queue) {
      process_receive_queue();
    }
  }
}

inline void comm::check_if_production_halt_required() {
  while (m_enable_interrupts && !m_in_process_receive_queue &&
         m_pending_isend_bytes > config.buffer_size) {
    process_receive_queue();
  }
}

/**
 * @brief Checks for incoming unless called from receive queue and flushes
 * one buffer.
 */
inline void comm::local_progress() {
  if (not m_in_process_receive_queue) {
    process_receive_queue();
  }
  if (not m_send_dest_queue.empty()) {
    int dest = m_send_dest_queue.front();
    m_send_dest_queue.pop_front();
    flush_send_buffer(dest);
  }
}

/**
 * @brief Waits until provided condition function returns true.
 *
 * @tparam Function
 * @param fn Wait condition function, must match []() -> bool
 */
template <typename Function>
inline void comm::local_wait_until(Function fn) {
  while (not fn()) {
    local_progress();
  }
}

/**
 * @brief Flushes all local state and buffers.
 * Notifies any registered barrier watchers.
 */
inline void comm::flush_all_local_and_process_incoming() {
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
inline void comm::flush_to_capacity() {
  while (m_send_buffer_bytes > config.buffer_size) {
    ASSERT_DEBUG(!m_send_dest_queue.empty());
    int dest = m_send_dest_queue.front();
    m_send_dest_queue.pop_front();
    flush_send_buffer(dest);
  }
}

inline void comm::post_new_irecv(std::shared_ptr<std::byte[]> &recv_buffer) {
  mpi_irecv_request recv_req;
  recv_req.buffer = recv_buffer;

  //::madvise(recv_req.buffer.get(), config.irecv_size, MADV_DONTNEED);
  ASSERT_MPI(MPI_Irecv(recv_req.buffer.get(), config.irecv_size, MPI_BYTE,
                       MPI_ANY_SOURCE, MPI_ANY_TAG, m_comm_async,
                       &(recv_req.request)));
  m_recv_queue.push_back(recv_req);
}

template <typename Lambda, typename... PackArgs>
inline size_t comm::pack_lambda(std::vector<std::byte> &packed, Lambda l,
                                const PackArgs &...args) {
  size_t                        size_before = packed.size();
  const std::tuple<PackArgs...> tuple_args(
      std::forward<const PackArgs>(args)...);

  auto dispatch_lambda = [](comm *c, cereal::YGMInputArchive *bia, Lambda l) {
    Lambda *pl = nullptr;
    size_t  l_storage[sizeof(Lambda) / sizeof(size_t) +
                     (sizeof(Lambda) % sizeof(size_t) > 0)];
    if constexpr (!std::is_empty<Lambda>::value) {
      bia->loadBinary(l_storage, sizeof(Lambda));
      pl = (Lambda *)l_storage;
    }

    std::tuple<PackArgs...> ta;
    if constexpr (!std::is_empty<std::tuple<PackArgs...>>::value) {
      (*bia)(ta);
    }

    auto t1 = std::make_tuple((comm *)c);

    // \pp was: std::apply(*pl, std::tuple_cat(t1, ta));
    ygm::meta::apply_optional(*pl, std::move(t1), std::move(ta));
  };

  return pack_lambda_generic(packed, l, dispatch_lambda,
                             std::forward<const PackArgs>(args)...);
}

template <typename Lambda, typename... PackArgs>
inline void comm::pack_lambda_broadcast(Lambda l, const PackArgs &...args) {
  const std::tuple<PackArgs...> tuple_args(
      std::forward<const PackArgs>(args)...);

  auto forward_remote_and_dispatch_lambda = [](comm                    *c,
                                               cereal::YGMInputArchive *bia,
                                               Lambda                   l) {
    Lambda *pl = nullptr;
    size_t  l_storage[sizeof(Lambda) / sizeof(size_t) +
                     (sizeof(Lambda) % sizeof(size_t) > 0)];
    if constexpr (!std::is_empty<Lambda>::value) {
      bia->loadBinary(l_storage, sizeof(Lambda));
      pl = (Lambda *)l_storage;
    }

    std::tuple<PackArgs...> ta;
    if constexpr (!std::is_empty<std::tuple<PackArgs...>>::value) {
      (*bia)(ta);
    }

    auto forward_local_and_dispatch_lambda =
        [](comm *c, cereal::YGMInputArchive *bia, Lambda l) {
          Lambda *pl = nullptr;
          size_t  l_storage[sizeof(Lambda) / sizeof(size_t) +
                           (sizeof(Lambda) % sizeof(size_t) > 0)];
          if constexpr (!std::is_empty<Lambda>::value) {
            bia->loadBinary(l_storage, sizeof(Lambda));
            pl = (Lambda *)l_storage;
          }

          std::tuple<PackArgs...> ta;
          if constexpr (!std::is_empty<std::tuple<PackArgs...>>::value) {
            (*bia)(ta);
          }

          auto local_dispatch_lambda = [](comm *c, cereal::YGMInputArchive *bia,
                                          Lambda l) {
            Lambda *pl = nullptr;
            size_t  l_storage[sizeof(Lambda) / sizeof(size_t) +
                             (sizeof(Lambda) % sizeof(size_t) > 0)];
            if constexpr (!std::is_empty<Lambda>::value) {
              bia->loadBinary(l_storage, sizeof(Lambda));
              pl = (Lambda *)l_storage;
            }

            std::tuple<PackArgs...> ta;
            if constexpr (!std::is_empty<std::tuple<PackArgs...>>::value) {
              (*bia)(ta);
            }

            auto t1 = std::make_tuple((comm *)c);

            // \pp was: std::apply(*pl, std::tuple_cat(t1, ta));
            ygm::meta::apply_optional(*pl, std::move(t1), std::move(ta));
          };

          // Pack lambda telling terminal ranks to execute user lambda.
          // TODO: Why does this work? Passing ta (tuple of args) to a function
          // expecting a parameter pack shouldn't work...
          std::vector<std::byte> packed_msg;
          c->pack_lambda_generic(packed_msg, *pl, local_dispatch_lambda, ta);

          for (auto dest : c->layout().local_ranks()) {
            if (dest != c->layout().rank()) {
              c->queue_message_bytes(packed_msg, dest);
            }
          }

          auto t1 = std::make_tuple((comm *)c);

          // \pp was: std::apply(*pl, std::tuple_cat(t1, ta));
          ygm::meta::apply_optional(*pl, std::move(t1), std::move(ta));
        };

    std::vector<std::byte> packed_msg;
    c->pack_lambda_generic(packed_msg, *pl, forward_local_and_dispatch_lambda,
                           ta);

    int num_layers = c->layout().node_size() / c->layout().local_size() +
                     (c->layout().node_size() % c->layout().local_size() > 0);
    int num_ranks_per_layer =
        c->layout().local_size() * c->layout().local_size();
    int node_partner_offset = (c->layout().local_id() - c->layout().node_id()) %
                              c->layout().local_size();

    // % operator is remainder, not actually mod. Need to fix result if result
    // was negative
    if (node_partner_offset < 0) {
      node_partner_offset += c->layout().local_size();
    }

    // Only forward remotely if initial remote node exists
    if (node_partner_offset < c->layout().node_size()) {
      int curr_partner = c->layout().strided_ranks()[node_partner_offset];
      for (int l = 0; l < num_layers; l++) {
        if (curr_partner >= c->layout().size()) {
          break;
        }
        if (!c->layout().is_local(curr_partner)) {
          c->queue_message_bytes(packed_msg, curr_partner);
        }

        curr_partner += num_ranks_per_layer;
      }
    }

    auto t1 = std::make_tuple((comm *)c);

    // \pp was: std::apply(*pl, std::tuple_cat(t1, ta));
    ygm::meta::apply_optional(*pl, std::move(t1), std::move(ta));
  };

  std::vector<std::byte> packed_msg;
  pack_lambda_generic(packed_msg, l, forward_remote_and_dispatch_lambda,
                      std::forward<const PackArgs>(args)...);

  // Initial send to all local ranks
  for (auto dest : layout().local_ranks()) {
    queue_message_bytes(packed_msg, dest);
  }
}

template <typename Lambda, typename RemoteLogicLambda, typename... PackArgs>
inline size_t comm::pack_lambda_generic(std::vector<std::byte> &packed,
                                        Lambda l, RemoteLogicLambda rll,
                                        const PackArgs &...args) {
  size_t                        size_before = packed.size();
  const std::tuple<PackArgs...> tuple_args(
      std::forward<const PackArgs>(args)...);

  auto remote_dispatch_lambda = [](comm *c, cereal::YGMInputArchive *bia) {
    RemoteLogicLambda *rll = nullptr;
    Lambda            *pl  = nullptr;

    (*rll)(c, bia, *pl);
  };

  uint16_t lid = m_lambda_map.register_lambda(remote_dispatch_lambda);

  {
    size_t size_before = packed.size();
    packed.resize(size_before + sizeof(lid));
    std::memcpy(packed.data() + size_before, &lid, sizeof(lid));
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
 * @brief Adds packed message directly to send buffer for specific
 * destination. Does not modify packed message to add headers for routing.
 *
 */
inline void comm::queue_message_bytes(const std::vector<std::byte> &packed,
                                      const int                     dest) {
  m_send_count++;

  //
  // add data to the dest buffer
  if (m_vec_send_buffers[dest].empty()) {
    m_send_dest_queue.push_back(dest);
    m_vec_send_buffers[dest].reserve(config.buffer_size / m_layout.node_size());
  }

  std::vector<std::byte> &send_buff = m_vec_send_buffers[dest];

  // Add dummy header with dest of -1 and size of 0.
  // This is to avoid peeling off and replacing the dest as messages are
  // forwarded in a bcast
  if (config.routing != detail::routing_type::NONE) {
    size_t header_bytes = pack_routing_header(send_buff, -1, 0);
    m_send_buffer_bytes += header_bytes;
  }

  size_t size_before = send_buff.size();
  send_buff.resize(size_before + packed.size());
  std::memcpy(send_buff.data() + size_before, packed.data(), packed.size());

  m_send_buffer_bytes += packed.size();
}

inline void comm::handle_next_receive(std::shared_ptr<std::byte[]> buffer,
                                      const size_t buffer_size) {
  cereal::YGMInputArchive iarchive(buffer.get(), buffer_size);
  while (!iarchive.empty()) {
    if (config.routing != detail::routing_type::NONE) {
      header_t h;
      iarchive.loadBinary(&h, sizeof(header_t));

      trace_header_t trace_h;
      TimeResolution event_time;
      if (config.trace_ygm) {
        event_time = m_tracer.get_time();
        iarchive.loadBinary(&trace_h, sizeof(trace_header_t));
      }

      if (h.dest == m_layout.rank() || (h.dest == -1 && h.message_size == 0)) {
        uint16_t lid;
        iarchive.loadBinary(&lid, sizeof(lid));
        m_lambda_map.execute(lid, this, &iarchive);
        m_recv_count++;
        stats.rpc_execute();

        // TODO: IMPLEMENTING Async 'e'
        if (config.trace_ygm) {
          TimeResolution duration = m_tracer.get_time() - event_time;
          std::unordered_map<std::string, std::any> metadata;
          metadata["from"]         = trace_h.from;
          metadata["to"]           = rank();
          metadata["event_id"]     = trace_h.trace_id;
          metadata["message_size"] = h.message_size;

          ConstEventType event_name = "async";
          ConstEventType action     = "receive";

          m_tracer.trace_event(trace_h.trace_id, action, event_name, rank(),
                               event_time, metadata, 'X', duration);
        }

      } else {
        int next_dest = m_router.next_hop(h.dest);

        if (m_vec_send_buffers[next_dest].empty()) {
          m_send_dest_queue.push_back(next_dest);
        }

        size_t header_bytes = pack_routing_header(m_vec_send_buffers[next_dest],
                                                  h.dest, h.message_size);
        m_send_buffer_bytes += header_bytes;

        size_t traciing_header_bytes = pack_tracing_header(
            m_vec_send_buffers[next_dest], trace_h.trace_id, 0);
        m_send_buffer_bytes += traciing_header_bytes;

        size_t precopy_size = m_vec_send_buffers[next_dest].size();
        m_vec_send_buffers[next_dest].resize(precopy_size + h.message_size);
        iarchive.loadBinary(&m_vec_send_buffers[next_dest][precopy_size],
                            h.message_size);

        m_send_buffer_bytes += h.message_size;

        flush_to_capacity();
      }
    } else {
      // TODO: load binary for tracing header if it exists
      trace_header_t trace_h;
      TimeResolution event_time;
      if (config.trace_ygm) {
        event_time = m_tracer.get_time();
        iarchive.loadBinary(&trace_h, sizeof(trace_header_t));
      }

      uint16_t lid;
      iarchive.loadBinary(&lid, sizeof(lid));
      m_lambda_map.execute(lid, this, &iarchive);
      m_recv_count++;
      stats.rpc_execute();

      // TODO: IMPLEMENTING Async 'e'
      if (config.trace_ygm) {
        TimeResolution duration = m_tracer.get_time() - event_time;
        std::unordered_map<std::string, std::any> metadata;
        metadata["from"]     = trace_h.from;
        metadata["to"]       = rank();
        metadata["event_id"] = trace_h.trace_id;

        ConstEventType event_name = "async";
        ConstEventType action     = "receive";

        m_tracer.trace_event(trace_h.trace_id, action, event_name, rank(),
                             event_time, metadata, 'X', duration);
      }
    }
  }
  post_new_irecv(buffer);
  flush_to_capacity();
}

/**
 * @brief Process receive queue of messages received by the listener thread.
 *
 * @return True if receive queue was non-empty, else false
 */
inline bool comm::process_receive_queue() {
  ASSERT_RELEASE(!m_in_process_receive_queue);
  m_in_process_receive_queue = true;
  bool received_to_return    = false;

  if (!m_enable_interrupts) {
    m_in_process_receive_queue = false;
    return received_to_return;
  }
  //
  // if we have a pending iRecv, then we can issue a Testsome
  if (m_send_queue.size() > config.num_isends_wait) {
    MPI_Request twin_req[2];
    twin_req[0] = m_send_queue.front().request;
    twin_req[1] = m_recv_queue.front().request;

    int        outcount{0};
    int        twin_indices[2];
    MPI_Status twin_status[2];
    {
      auto timer = stats.waitsome_isend_irecv();
      while (outcount == 0) {
        ASSERT_MPI(
            MPI_Testsome(2, twin_req, &outcount, twin_indices, twin_status));
      }
    }
    for (int i = 0; i < outcount; ++i) {
      if (twin_indices[i] == 0) {  // completed a iSend
        send_queue_completed++;
        if (config.trace_mpi) {
          TimeResolution event_time = m_tracer.get_time();
          std::unordered_map<std::string, std::any> metadata;
          metadata["type"]          = "mpi_send";
          ConstEventType event_name = "mpi";
          ConstEventType action     = "mpi_send";
          m_tracer.trace_event(m_send_queue.front().id, action, event_name,
                               rank(), event_time, metadata, 'e');
        }

        m_pending_isend_bytes -= m_send_queue.front().buffer->size();
        m_send_queue.front().buffer->clear();
        m_free_send_buffers.push_back(m_send_queue.front().buffer);
        m_send_queue.pop_front();
      } else {  // completed an iRecv -- COPIED FROM BELOW
        receive_queue_completed++;
        received_to_return           = true;
        mpi_irecv_request req_buffer = m_recv_queue.front();
        m_recv_queue.pop_front();
        int buffer_size{0};
        ASSERT_MPI(MPI_Get_count(&twin_status[i], MPI_BYTE, &buffer_size));
        stats.irecv(twin_status[i].MPI_SOURCE, buffer_size);
        handle_next_receive(req_buffer.buffer, buffer_size);
      }
    }
  } else {
    if (!m_send_queue.empty()) {
      int flag(0);
      ASSERT_MPI(
          MPI_Test(&(m_send_queue.front().request), &flag, MPI_STATUS_IGNORE));
      stats.isend_test();
      if (flag) {
        send_queue_completed++;
        if (config.trace_mpi) {
          TimeResolution event_time = m_tracer.get_time();
          std::unordered_map<std::string, std::any> metadata;
          metadata["type"]          = "mpi_send";
          ConstEventType event_name = "mpi";
          ConstEventType action     = "mpi_send";
          m_tracer.trace_event(m_send_queue.front().id, action, event_name,
                               rank(), event_time, metadata, 'e');
        }
        m_pending_isend_bytes -= m_send_queue.front().buffer->size();
        m_send_queue.front().buffer->clear();
        m_free_send_buffers.push_back(m_send_queue.front().buffer);
        m_send_queue.pop_front();
      }
    }
  }

  received_to_return != local_process_incoming();

  m_in_process_receive_queue = false;
  return received_to_return;
}

inline bool comm::local_process_incoming() {
  bool received_to_return = false;

  while (true) {
    int        flag(0);
    MPI_Status status;
    ASSERT_MPI(MPI_Test(&(m_recv_queue.front().request), &flag, &status));
    stats.irecv_test();
    if (flag) {
      receive_buffer_count++;
      if (config.trace_mpi) {
        TimeResolution event_time = m_tracer.get_time();
        std::unordered_map<std::string, std::any> metadata;
        metadata["type"] = "local_process_incoming";

        ConstEventType event_name = "mpi";
        ConstEventType action     = "mpi_receive";

        m_tracer.trace_event(0, action, event_name, rank(), event_time,
                             metadata);
      }
      received_to_return           = true;
      mpi_irecv_request req_buffer = m_recv_queue.front();
      m_recv_queue.pop_front();
      int buffer_size{0};
      ASSERT_MPI(MPI_Get_count(&status, MPI_BYTE, &buffer_size));
      stats.irecv(status.MPI_SOURCE, buffer_size);
      handle_next_receive(req_buffer.buffer, buffer_size);
    } else {
      break;  // not ready yet
    }
  }
  return received_to_return;
}
};  // namespace ygm
