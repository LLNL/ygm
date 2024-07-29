// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once

#include <iomanip>
#include <string>
#include <variant>
#include <vector>

namespace ygm::io::detail {

class csv_field {
 public:
  csv_field(const std::string &f) : m_f(f) {}

  bool is_integer() const {
    int64_t            test;
    std::cout << m_f << std::endl;
    std::istringstream ss{m_f};
    ss >> test;
    return !ss.fail() && ss.eof();
  }

  int64_t as_integer() const {
    int64_t            to_return;
    std::istringstream ss{m_f};
    ss >> to_return;
    assert(!ss.fail() && ss.eof());
    return to_return;
  }

  bool is_unsigned_integer() const {
    uint64_t           test;
    std::istringstream ss{m_f};
    ss >> test;
    return !ss.fail() && ss.eof();
  }

  uint64_t as_unsigned_integer() const {
    uint64_t           to_return;
    std::istringstream ss{m_f};
    ss >> to_return;
    assert(!ss.fail() && ss.eof());
    return to_return;
  }

  bool is_double() const {
    double             test;
    std::istringstream ss{m_f};
    ss >> test;
    return !ss.fail() && ss.eof();
  }

  double as_double() const {
    double             to_return;
    std::istringstream ss{m_f};
    ss >> to_return;
    assert(!ss.fail() && ss.eof());
    return to_return;
  }

  const std::string &as_string() const { return m_f; }

 private:
  friend std::ostream &operator<<(std::ostream &os, const csv_field &f);
  std::string          m_f;
};

std::ostream &operator<<(std::ostream &os, const csv_field &f) {
  return os << f.as_string();
}

std::vector<csv_field> parse_csv_line(const std::string line) {
  std::vector<csv_field> line_fields;
  if (line.empty() || line[0] == '#') {
    return line_fields;
  }
  std::stringstream ssline{line};
  while (ssline >> std::ws) {
    std::string csv_field;
    if (ssline.peek() == '"') {
      ssline >> std::quoted(csv_field);
      if (ssline) {
        line_fields.push_back(csv_field);
      }
      ssline.ignore(256, ',');
    } else {
      std::getline(ssline, csv_field, ',');
      if (ssline) {
        line_fields.push_back(csv_field);
      }
    }
  }
  return line_fields;
}

std::string convert_type_string(const std::vector<csv_field> &line_fields) {
  std::stringstream ss;
  for (const auto &f : line_fields) {
    if (f.is_integer()) {
      ss << "I";
    } else if (f.is_double()) {
      ss << "D";
    } else {
      ss << "S";
    }
  }
  return ss.str();
}

}  // namespace ygm::io::detail
