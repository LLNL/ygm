// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once

#include <fstream>
#include <string>
#include <vector>
#include <ygm/io/detail/csv.hpp>
#include <ygm/io/line_parser.hpp>

namespace ygm::io {

class csv_parser {
 public:
  template <typename... Args>
  csv_parser(Args&&... args) : m_lp(std::forward<Args>(args)...) {}

  /**
   * @brief Executes a user function for every CSV record in a set of files.
   *
   * @tparam Function
   * @param fn User function to execute
   */
  template <typename Function>
  void for_all(Function fn) {
    using namespace ygm::io::detail;
    m_lp.for_all([fn](const std::string& line) {
      auto vfields = parse_csv_line(line);
      // auto stypes    = convert_type_string(vfields);
      // todo, detect if types are inconsistent between records
      if (vfields.size() > 0) {
        fn(vfields);
      }
    });
  }

 private:
  line_parser m_lp;
};

}  // namespace ygm::io
