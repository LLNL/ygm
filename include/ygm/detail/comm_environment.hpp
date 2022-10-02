// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once

#include <cstdlib>
#include <sstream>
#include <string>

namespace ygm {

namespace detail {

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
    if (const char* cc = std::getenv("YGM_COMM_ROUTING")) {
      if (std::string(cc) == "NONE") {
        routing = NONE;
      } else if (std::string(cc) == "NR") {
        routing = NR;
      } else if (std::string(cc) == "NLNR") {
        routing = NLNR;
      } else {
        throw std::runtime_error("comm_enviornment -- unknown routing type");
      }
    }
  }

  void print(std::ostream& os = std::cout) const {
    os << "======== ENVIRONMENT SETTINGS ========\n"
       << "YGM_COMM_BUFFER_SIZE_KB  = " << buffer_size / 1024 << "\n"
       << "YGM_COMM_NUM_IRECVS      = " << num_irecvs << "\n"
       << "YGM_COMM_IRECVS_SIZE_KB  = " << irecv_size / 1024 << "\n"
       << "YGM_COMM_NUM_ISENDS_WAIT = " << num_isends_wait << "\n"
       << "YGM_COMM_ROUTING         = ";
    switch (routing) {
      case NONE:
        os << "NONE\n";
        break;
      case NR:
        os << "NR\n";
        break;
      case NLNR:
        os << "NLNR\n";
        break;
    }
    os << "======================================\n";
  }

  //
  // variables with their default values
  size_t buffer_size = 16 * 1024 * 1024;

  size_t irecv_size = 1024 * 1024 * 1024;
  size_t num_irecvs = 8;

  size_t num_isends_wait = 4;

  enum routing_type { NONE = 0, NR = 1, NLNR = 2 };
  routing_type routing = NONE;

  bool welcome = false;
};

}  // namespace detail
}  // namespace ygm
