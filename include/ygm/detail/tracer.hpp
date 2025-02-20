#pragma once

#include <iostream>
#include <fstream>
#include <filesystem>
#include <variant>
#include <cereal/types/variant.hpp>
#include <cereal/archives/binary.hpp>
#include <cereal/types/string.hpp>

namespace ygm::detail {

  // YGM Async
  struct ygm_async {
    uint64_t event_id;
    int to;
    uint32_t message_size;

    template <class Archive>
    void serialize(Archive & ar) {
        ar(event_id to, message_size);
    }
  };

  // MPI Send
  struct YGMEvent {
    uint64_t event_id;
    int to;
    uint32_t buffer_size;

    template <class Archive>
    void serialize(Archive & ar) {
        ar(event_id, to, buffer_size);
    }
  };

  // MPI Receive

  // Barrier Begin

  // Barrier End

  struct YGMEvent {
    uint64_t event_id;
    int from;
    int to;
    uint32_t message_size;
    char type; // TODO: Get rid of
    char action;

    template <class Archive>
    void serialize(Archive & ar) {
        ar(event_id, from, to, message_size, type, action);
    }
};

struct YGMBarrierEvent {
    uint64_t event_id;
    int rank; // TODO: get rid of
    uint64_t send_count;
    uint64_t recv_count;
    size_t pending_isend_bytes; // 
    size_t send_buffer_bytes;

    template <class Archive>
    void serialize(Archive & ar) {
        ar(event_id, rank, send_count, recv_count, pending_isend_bytes, send_buffer_bytes);
    }
};

struct VariantEvent {
  std::variant<YGMEvent, YGMBarrierEvent> data {};
  template< class Archive >
  void serialize( Archive & archive ) {
      archive( data );
  }
};

// Tracer class with simplified event handling
class tracer {
 public:
  tracer() = default;

  ~tracer() {
    if (output_file.is_open()) {
      output_file.close();
      if (output_file.fail()) {
        std::cerr << "Error closing trace file!" << std::endl;
      }
    }
  }

  void create_directory(const std::string& trace_path) {
    if (!std::filesystem::is_directory(trace_path)) {
      if (!std::filesystem::create_directories(trace_path)) {
        std::cerr << "Error creating directory!" << std::endl;
      }
    }
  }

  void open_file(const std::string& trace_path, int comm_rank, int comm_size) {
    m_comm_size = comm_size;
    m_rank = comm_rank;
    std::string file_path = trace_path + "/trace_" + std::to_string(comm_rank) + ".bin";
    output_file.open(file_path, std::ios::binary);

    if (!output_file.is_open()) {
      std::cerr << "Error opening " << file_path << " for writing!" << std::endl;
    }
  }

  // Function to generate the next unique message id
  int get_next_message_id() { 
    return m_next_message_id += m_comm_size; 
  }

  // Loging an event
  template <typename EventType>
  void log_event(const EventType& event) {
    cereal::BinaryOutputArchive oarchive(output_file);
    VariantEvent variant_event {event};  
    oarchive(variant_event);
  }

  // Trace functions for specific event types
  void trace_ygm_async(uint64_t id, int dest, uint32_t bytes) {
    YGMEvent event;
    event.event_id = id;

    event.from = m_rank;
    event.to = dest;
    event.message_size = bytes;

    event.type = 'y';
    event.action = 's' ;

    log_event(event);
  }

  void trace_ygm_async_recv(uint64_t id, int from, uint32_t bytes) {
    YGMEvent event;
    event.event_id = id;

    event.from = from;
    event.to = m_rank;
    event.message_size = bytes;

    event.type = 'y';
    event.action = 'r' ;

    log_event(event);
  }

  void trace_mpi_send(uint64_t id, int dest, uint32_t bytes) {
    YGMEvent event;
    event.event_id = id;

    event.from = m_rank;
    event.to = dest;
    event.message_size = bytes;

    event.type = 'm';
    event.action = 's' ;

    log_event(event);
  }

  void trace_mpi_recv(uint64_t id, int from, uint32_t bytes) {
    YGMEvent event;
    event.event_id = id;

    event.from = from;
    event.to = m_rank;
    event.message_size = bytes;

    event.type = 'm';
    event.action = 'r' ;

    log_event(event);
  }

  void trace_barrier(uint64_t id, uint64_t send_count,
                     uint64_t recv_count, size_t pending_isend_bytes, size_t send_buffer_bytes) {
    YGMBarrierEvent event;
    event.event_id = id;
    event.rank = m_rank;
    event.send_count = send_count;
    event.recv_count = recv_count;
    event.pending_isend_bytes = pending_isend_bytes;
    event.send_buffer_bytes = send_buffer_bytes;

    log_event(event);
  }

 private:
  std::ofstream output_file;
  int m_comm_size = 0;
  int m_rank = -1;
  int m_next_message_id = 0;
};

}  // namespace ygm::detail
