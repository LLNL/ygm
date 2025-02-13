// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once

#include <cstdlib>
#include <iostream>
#include <sstream>
#include <string>
#include <cmath>
#include <ygm/detail/layout.hpp>

namespace ygm {

namespace detail {

enum class routing_type { NONE, NR, NLNR };

  size_t round_to_nearest_kb(float number) {
    return std::ceil(static_cast<float>(number) / 1024) * 1024;
  }
  
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
  comm_environment(const ygm::detail::layout& layout) {
    size_t nodes = layout.node_size();
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
    if (const char* cc = std::getenv("YGM_COM_BUFFER_SIZE_KB")) {
      total_buffer_size = convert<size_t>(cc) * 1024;
    }
    switch (routing) {
      case routing_type::NONE :
        local_buffer_size  = round_to_nearest_kb((float) total_buffer_size / nodes);
        remote_buffer_size = total_buffer_size - local_buffer_size;
        break;
      case routing_type::NR :
        local_buffer_size  = round_to_nearest_kb((float) total_buffer_size / 2);
        remote_buffer_size = local_buffer_size;
        break;
      case routing_type::NLNR :
        local_buffer_size  = round_to_nearest_kb(2 * (float) total_buffer_size / 3);
        remote_buffer_size = round_to_nearest_kb((float) total_buffer_size / 3);
        break;
    }
    if (const char* cc = std::getenv("YGM_COMM_LOCAL_BUFFER_SIZE_KB")) {
      local_buffer_size = convert<size_t>(cc) * 1024;
      if (std::getenv("YGM_COMM_REMOTE_BUFFER_SIZE_KB") == nullptr)
        std::cerr << "YGM_COMM_REMOTE_BUFFER_SIZE_KB not set, using recommended value of" << remote_buffer_size << "\n";
    }
    if (const char* cc = std::getenv("YGM_COMM_REMOTE_BUFFER_SIZE_KB")) {
      remote_buffer_size = convert<size_t>(cc) * 1024;
      if (std::getenv("YGM_COMM_LOCAL_BUFFER_SIZE_KB") == nullptr)
        std::cerr << "YGM_COMM_LOCAL_BUFFER_SIZE_KB not set, using recommended value of" << local_buffer_size << "\n";
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
  }

  void print(std::ostream& os = std::cout) const {
    os << "======== ENVIRONMENT SETTINGS ========\n"
       << "YGM_COMM_LOCAL_BUFFER_SIZE_KB   = " << local_buffer_size / 1024 << "\n"
       << "YGM_COMM_REMOTE_BUFFER_SIZE_KB  = " << remote_buffer_size / 1024 << "\n"
       << "YGM_COMM_NUM_IRECVS             = " << num_irecvs << "\n"
       << "YGM_COMM_IRECVS_SIZE_KB         = " << irecv_size / 1024 << "\n"
       << "YGM_COMM_NUM_ISENDS_WAIT        = " << num_isends_wait << "\n"
       << "YGM_COMM_ISSEND_FREQ            = " << freq_issend << "\n"
       << "YGM_COMM_ROUTING                = ";
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
    os << "======================================\n";
  }

  //
  // variables with their default values
  size_t total_buffer_size = 16 * 1024 * 1024;
  size_t local_buffer_size;
  size_t remote_buffer_size;

  size_t irecv_size = 1024 * 1024 * 1024;
  size_t num_irecvs = 8;

  size_t num_isends_wait           = 4;
  size_t freq_issend               = 8;
  size_t send_buffer_free_list_len = 32;

  routing_type routing = routing_type::NONE;

  bool welcome = false;
};

}  // namespace detail
}  // namespace ygm
