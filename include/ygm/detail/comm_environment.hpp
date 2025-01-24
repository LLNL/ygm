// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once

#include <cstdlib>
#include <iostream>
#include <sstream>
#include <string>

namespace ygm {

namespace detail {

enum class routing_type { NONE, NR, NLNR };

/**
 * @brief Configuration enviornment for ygm::comm.
 *
 * @todo Add support for a ".ygm" file and YGM_CONFIG_FILE.
 */
class comm_environment {
  /**
   * @brief Converts char* to type T
   */
  template <typename T>
  T convert(const char* str) {
    std::stringstream sstr;
    sstr << str;
    T to_return;
    sstr >> to_return;
    return to_return;
  }

 public:
  comm_environment() {
    if (const char* cc = std::getenv("YGM_COMM_BUFFER_SIZE_KB")) {
      buffer_size = convert<size_t>(cc) * 1024;
    }
    if (const char* cc = std::getenv("YGM_COMM_NUM_IRECVS")) {
      num_irecvs = convert<size_t>(cc);
    }
    if (const char* cc = std::getenv("YGM_COMM_IRECV_SIZE_KB")) {
      irecv_size = convert<size_t>(cc) * 1024;
    }
    if (const char* cc = std::getenv("YGM_COMM_WELCOME")) {
      welcome = convert<bool>(cc);
    }
    if (const char* cc = std::getenv("YGM_COMM_NUM_ISENDS_WAIT")) {
      num_isends_wait = convert<size_t>(cc);
    }
    if (const char* cc = std::getenv("YGM_COMM_ISSEND_FREQ")) {
      freq_issend = convert<size_t>(cc);
    }
    if (const char* cc = std::getenv("YGM_COMM_SEND_BUFFER_FREE_LIST_LEN")) {
      send_buffer_free_list_len = convert<size_t>(cc);
    }
    if (const char* cc = std::getenv("YGM_COMM_ROUTING")) {
      if (std::string(cc) == "NONE") {
        routing = routing_type::NONE;
      } else if (std::string(cc) == "NR") {
        routing = routing_type::NR;
      } else if (std::string(cc) == "NLNR") {
        routing = routing_type::NLNR;
      } else {
        throw std::runtime_error("comm_enviornment -- unknown routing type");
      }
    }
    if (const char* cc = std::getenv("YGM_COMM_TRACE")) {
      trace_ygm = convert<bool>(cc);
    }
    if (const char* cc = std::getenv("YGM_MPI_TRACE")) {
      trace_mpi = convert<bool>(cc);
    }
    if (const char* cc = std::getenv("YGM_COMM_TRACE_PATH")) {
      trace_path = std::string(cc);
    }
  }

  void print(std::ostream& os = std::cout) const {
    os << "======== ENVIRONMENT SETTINGS ========\n"
       << "YGM_COMM_BUFFER_SIZE_KB  = " << buffer_size / 1024 << "\n"
       << "YGM_COMM_NUM_IRECVS      = " << num_irecvs << "\n"
       << "YGM_COMM_IRECVS_SIZE_KB  = " << irecv_size / 1024 << "\n"
       << "YGM_COMM_NUM_ISENDS_WAIT = " << num_isends_wait << "\n"
       << "YGM_COMM_ISSEND_FREQ     = " << freq_issend << "\n"
       << "YGM_COMM_ROUTING         = ";
    switch (routing) {
      case routing_type::NONE:
        os << "NONE\n";
        break;
      case routing_type::NR:
        os << "NR\n";
        break;
      case routing_type::NLNR:
        os << "NLNR\n";
        break;
    }
    os << "YGM_COMM_TRACE           = " << trace_ygm << "\n";
    os << "YGM_MPI_TRACE           = " << trace_mpi << "\n";
    os << "======================================\n";
  }

  //
  // variables with their default values
  size_t buffer_size = 16 * 1024 * 1024;

  size_t irecv_size = 1024 * 1024 * 1024;
  size_t num_irecvs = 8;

  size_t num_isends_wait           = 4;
  size_t freq_issend               = 8;
  size_t send_buffer_free_list_len = 32;

  routing_type routing = routing_type::NONE;

  bool welcome = false;

  bool        trace_ygm  = false;
  bool        trace_mpi  = false;
  std::string trace_path = "trace/";
};

}  // namespace detail
}  // namespace ygm
