// Copyright 2019-2023 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once

#include <algorithm>
#include <iostream>
#include <iterator>
#include <stdexcept>
#include <string>
#include <unordered_map>

#include <parquet/types.h>

// TODO: support the link mode
#if !__has_include(<boost/json/src.hpp>)
#error BOOST >= 1.75 is required for Boost.JSON
#endif
#include <boost/json/src.hpp>

#include <ygm/io/arrow_parquet_parser.hpp>

namespace ygm::io::detail {
inline boost::json::value read_parquet_element_as_json_value(
    const ygm::io::parquet_data_type& type_holder,
    arrow_parquet_parser::parquet_stream_reader& stream) {
  boost::json::value out_value;
  out_value.emplace_null();
  switch (type_holder.type) {
    case parquet::Type::BOOLEAN:
      stream >> out_value.emplace_bool();
      break;
    case parquet::Type::INT32:
      stream >> out_value.emplace_int64();
      break;
    case parquet::Type::INT64:
      stream >> out_value.emplace_int64();
      break;
    case parquet::Type::DOUBLE:
      stream >> out_value.emplace_double();
      break;
    case parquet::Type::FLOAT:
      stream >> out_value.emplace_double();
      break;
    case parquet::Type::BYTE_ARRAY: {
      std::string buf;
      stream >> buf;
      out_value.emplace_string() = buf;
      break;
    }
    case parquet::Type::FIXED_LEN_BYTE_ARRAY: {
      throw std::runtime_error("FIXED_LEN_BYTE_ARRAY is not supported");
      break;
    }
    case parquet::Type::INT96:
      throw std::runtime_error("INT96 is not supported");
      break;
    case parquet::Type::UNDEFINED:
      [[fallthrough]];
    default:
      throw std::runtime_error("Undefined data type");
      break;
  }

  return out_value;
}

template <typename key_container>
inline boost::json::object read_parquet_as_json_helper(
    arrow_parquet_parser::parquet_stream_reader&       reader,
    const arrow_parquet_parser::file_schema_container& schema,
    const bool                                         read_all,
    const key_container& include_columns = key_container()) {
  boost::json::object object;
  for (size_t i = 0; i < schema.size(); ++i) {
    const auto& data_type   = std::get<0>(schema[i]);
    const auto& colum_name  = std::get<1>(schema[i]);
    if (!read_all &&
        std::find(std::begin(include_columns), std::end(include_columns),
                  colum_name) == std::end(include_columns)) {
      continue;
    }
    object[colum_name] =
        read_parquet_element_as_json_value(data_type, reader);
  }
  reader.SkipColumns(schema.size());
  reader.EndRow();
  return object;
}

/**
 * @brief Reads a row data from a Parquet StreamReader and returns the read row
 * as a Boost.JSON object instance. JSON object keys are Parquet column names.
 * Only supports the plain encoding. Do not support nested or hierarchical
 * columns.
 */
inline boost::json::object read_parquet_as_json(
    arrow_parquet_parser::parquet_stream_reader&       reader,
    const arrow_parquet_parser::file_schema_container& schema) {
  return read_parquet_as_json_helper<std::unordered_set<std::string>>(
      reader, schema, true);
}

/**
 * @brief read_parquet_as_json() function with a list/set of column names
 * (string) to include.
 */
template <typename key_container>
inline boost::json::object read_parquet_as_json(
    arrow_parquet_parser::parquet_stream_reader&       reader,
    const arrow_parquet_parser::file_schema_container& schema,
    const key_container&                               include_columns) {
  return read_parquet_as_json_helper(reader, schema, false, include_columns);
}
}  // namespace ygm::io::detail