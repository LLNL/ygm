#pragma once

#include <sys/time.h>
#include <syscall.h>
#include <unistd.h>
#include <atomic>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <stack>
#include <string>
#include <unordered_map>

#include <any>

using ProcessID      = unsigned long int;
using ThreadID       = unsigned long int;
using TimeResolution = unsigned long long int;
using EventType      = char *;
using ConstEventType = const char *;

namespace ygm::detail {

class tracer {
 public:
  tracer() {}

  ~tracer() {
    if (output_file.is_open()) {
      output_file.close();
      if (output_file.fail()) {
        std::cerr << "Error closing trace file!" << std::endl;
        // Handle failure to close the file
      }
    }
  }

  inline TimeResolution get_time() {
    struct timeval tv {};
    gettimeofday(&tv, NULL);
    TimeResolution t = 1000000 * tv.tv_sec + tv.tv_usec;
    return t;
  }

  void create_directory(std::string trace_path) {
    if (!std::filesystem::is_directory(trace_path)) {
      if (!std::filesystem::create_directories(trace_path)) {
        std::cerr << "Error creating directory!" << std::endl;
      }
    }
  }

  void open_file(std::string trace_path, int comm_rank, int comm_size) {
    m_next_message_id = comm_rank;
    m_comm_size = comm_size; 
    m_rank = comm_rank;
    std::string file_path =
        trace_path + "/trace_" + std::to_string(comm_rank) + ".txt";
    ;

    output_file.open(file_path);

    if (!output_file.is_open()) {
      std::cerr << "Error opening " << file_path << " for writing!"
                << std::endl;
    }
  }

  int get_next_message_id(){ 
    return m_next_message_id += m_comm_size;
  }

  void trace_event(uint64_t event_id, ConstEventType action,
                   ConstEventType event_name, int rank,
                   TimeResolution                            start_time,
                   std::unordered_map<std::string, std::any> metadata,
                   char event_type = 'X', TimeResolution duration = 0) {

    ConstEventType category = "ygm";

    metadata["event_id"] = event_id;
    std::string meta_str = stream_metadata(metadata);

    size = snprintf(
        data, MAX_LINE_SIZE,
        "{\"id\":\"%lu\",\"name\":\"%s\",\"cat\":\"%s\",\"pid\":\"%lu\","
        "\"tid\":\"%s\",\"ts\":\"%llu\",\"dur\":\"%llu\",\"ph\":\"%c\","
        "\"args\":{%s}},\n",
        event_id, event_name, category, rank, action, start_time, duration,
        event_type, meta_str.c_str());

    output_file.write(data, size); 
  }

  void trace_ygm_async(u_int64_t id, int dest, u_int32_t bytes, TimeResolution event_time){
    TimeResolution duration = get_time() - event_time;

    std::unordered_map<std::string, std::any> metadata;
    metadata["from"]         = m_rank;
    metadata["to"]           = dest;
    metadata["message_size"] = bytes;

    ConstEventType event_name = "async";
    ConstEventType action     = "send";

    trace_event(id, action, event_name, m_rank,
                event_time, metadata, 'X', duration);
  }

  void trace_ygm_async_recv(u_int64_t id, int from, u_int32_t bytes, TimeResolution event_time){
    TimeResolution duration = get_time() - event_time;

    std::unordered_map<std::string, std::any> metadata;
    metadata["from"]         = from;
    metadata["to"]           = m_rank;
    metadata["message_size"] = bytes;

    ConstEventType event_name = "async";
    ConstEventType action     = "receive";

    trace_event(id ,action, event_name, m_rank,
                        event_time, metadata, 'X', duration);    
  }



  void trace_barrier(u_int64_t id, TimeResolution start_time, u_int64_t send_count ,u_int64_t recv_count, size_t pending_isend_bytes, size_t send_buffer_bytes){
    TimeResolution duration = get_time() - start_time;

    std::unordered_map<std::string, std::any> metadata;
    metadata["m_send_count"]          = send_count;
    metadata["m_recv_count"]          = recv_count;
    metadata["m_pending_isend_bytes"] = pending_isend_bytes;
    metadata["m_send_buffer_bytes"]   = send_buffer_bytes;
    ConstEventType event_name         = "barrier";
    ConstEventType action             = "barrier";

    trace_event(id, action, event_name, m_rank,
                start_time, metadata, 'X', duration);
  }

  void trace_mpi_receive(u_int64_t id, int from, int buffer_size){
    TimeResolution event_time = get_time();

    std::unordered_map<std::string, std::any> metadata;
    metadata["type"] = "barrier_reduce_counts";
    metadata["from"] = from;
    metadata["size"] = buffer_size;

    ConstEventType event_name = "mpi_receive";
    ConstEventType action     = "mpi_receive";

    trace_event(0, action, event_name, m_rank, event_time,
                        metadata);
  }

  void trace_mpi_send(u_int64_t id, int to, int buffer_size){
    TimeResolution event_time = get_time();

    std::unordered_map<std::string, std::any> metadata;
    metadata["type"] = "mpi_send";
    metadata["to"] = to;
    metadata["size"] = buffer_size;

    ConstEventType event_name = "mpi_send";
    ConstEventType action     = "mpi_send";

    trace_event(id, action, event_name, m_rank,
                        event_time, metadata);
  }
  

 private:
  std::ofstream output_file;
  int m_comm_size = 0;
  int m_rank = -1;
  int m_next_message_id = 0;

  static const int MAX_LINE_SIZE      = 4096;
  static const int MAX_META_LINE_SIZE = 3000;

  char data[MAX_LINE_SIZE];
  int  size = 0;

  std::string stream_metadata(
      const std::unordered_map<std::string, std::any> &metadata) {
    std::stringstream meta_stream;
    bool              has_meta = false;
    size_t            i        = 0;

    for (const auto &item : metadata) {
      if (has_meta) {
        meta_stream << ",";
      }

      try {
        meta_stream << "\"" << item.first << "\":\"";

        if (item.second.type() == typeid(unsigned int)) {
          const auto &value = std::any_cast<unsigned int>(item.second);
          meta_stream << value;
        } else if (item.second.type() == typeid(int)) {
          const auto &value = std::any_cast<int>(item.second);
          meta_stream << value;
        } else if (item.second.type() == typeid(const char *)) {
          const auto &value = std::any_cast<const char *>(item.second);
          meta_stream << value;
        } else if (item.second.type() == typeid(std::string)) {
          const auto &value = std::any_cast<std::string>(item.second);
          meta_stream << value;
        } else if (item.second.type() == typeid(size_t)) {
          const auto &value = std::any_cast<size_t>(item.second);
          meta_stream << value;
        } else if (item.second.type() == typeid(long)) {
          const auto &value = std::any_cast<long>(item.second);
          meta_stream << value;
        } else if (item.second.type() == typeid(ssize_t)) {
          const auto &value = std::any_cast<ssize_t>(item.second);
          meta_stream << value;
        } else if (item.second.type() == typeid(off_t)) {
          const auto &value = std::any_cast<off_t>(item.second);
          meta_stream << value;
        } else if (item.second.type() == typeid(off64_t)) {
          const auto &value = std::any_cast<off64_t>(item.second);
          meta_stream << value;
        } else if (item.second.type() == typeid(float)) {
          const auto &value = std::any_cast<float>(item.second);
          meta_stream << value;
        } else {
          meta_stream << "No conversion for " << item.first << "'s type";
        }

      } catch (const std::bad_any_cast &) {
        meta_stream << "No conversion for type";
      }
      meta_stream << "\"";
      has_meta = true;
      ++i;
    }
    return meta_stream.str();
  }
};
};  // namespace ygm::detail