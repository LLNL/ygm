// Copyright 2019-2025 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once

#include <algorithm>
#include <iostream>
#include <iterator>
#include <optional>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <variant>

#include <parquet/types.h>

#include <ygm/io/parquet_parser.hpp>

namespace ygm::io {

using parquet_type_variant = std::variant<std::monostate, bool, int32_t,
                                          int64_t, float, double, std::string>;

namespace detail {
inline parquet_type_variant read_parquet_element_as_variant(
    const ygm::io::detail::parquet_data_type& type_holder,
    parquet_parser::parquet_stream_reader&    stream) {
  parquet_type_variant out_value = std::monostate{};

  // Note: there is no uint types in Parquet
  if (type_holder.type == parquet::Type::BOOLEAN) {
    std::optional<bool> buf;
    stream >> buf;
    if (buf) {
      out_value = buf.value();
    }
  } else if (type_holder.type == parquet::Type::INT32) {
    std::optional<int32_t> buf;
    stream >> buf;
    if (buf) {
      out_value = buf.value();
    }
  } else if (type_holder.type == parquet::Type::INT64) {
    std::optional<int64_t> buf;
    stream >> buf;
    if (buf) {
      out_value = buf.value();
    }
  } else if (type_holder.type == parquet::Type::FLOAT) {
    std::optional<float> buf;
    stream >> buf;
    if (buf) {
      out_value = buf.value();
    }
  } else if (type_holder.type == parquet::Type::DOUBLE) {
    std::optional<double> buf;
    stream >> buf;
    if (buf) {
      out_value = buf.value();
    }
  } else if (type_holder.type == parquet::Type::BYTE_ARRAY) {
    std::optional<std::string> buf;
    stream >> buf;
    if (buf) {
      out_value = buf.value();
    }
  } else if (type_holder.type == parquet::Type::FIXED_LEN_BYTE_ARRAY) {
    throw std::runtime_error("FIXED_LEN_BYTE_ARRAY is not supported");
  } else if (type_holder.type == parquet::Type::INT96) {
    throw std::runtime_error("INT96 is not supported");
  } else {
    throw std::runtime_error("Unknown parquet::Type data type");
  }

  return out_value;
}

inline std::vector<parquet_type_variant> read_parquet_as_variant_helper(
    parquet_parser::parquet_stream_reader&                reader,
    const parquet_parser::file_schema_container&          schema,
    const std::optional<std::unordered_set<std::string>>& include_columns =
        std::nullopt) {
  std::vector<parquet_type_variant> row;
  for (size_t i = 0; i < schema.size(); ++i) {
    const auto& data_type  = std::get<0>(schema[i]);
    const auto& colum_name = std::get<1>(schema[i]);
    if (include_columns &&
        std::find(std::begin(*include_columns), std::end(*include_columns),
                  colum_name) == std::end(*include_columns)) {
      reader.SkipColumns(1);
      continue;
    }
    if (data_type.unsupported) {
      // Skip unsupported columns instead of throwing an exception
      reader.SkipColumns(1);
      continue;
    }
    row.emplace_back(read_parquet_element_as_variant(data_type, reader));
  }
  reader.SkipColumns(schema.size());
  reader.EndRow();
  return row;
}
}  // namespace detail

/**
 * @brief Reads a row data from a Parquet StreamReader and returns the read row
 * as an std::vector<parquet_type_variant> object.
 * Only supports the plain encoding. Do not support nested or hierarchical
 * columns.
 */
inline std::vector<parquet_type_variant> read_parquet_as_variant(
    parquet_parser::parquet_stream_reader&       reader,
    const parquet_parser::file_schema_container& schema) {
  return detail::read_parquet_as_variant_helper(reader, schema);
}

/**
 * @brief read_parquet_as_variant() function with a list/set of column names
 * (string) to include.
 */
inline std::vector<parquet_type_variant> read_parquet_as_variant(
    parquet_parser::parquet_stream_reader&       reader,
    const parquet_parser::file_schema_container& schema,
    const std::unordered_set<std::string>&       include_columns) {
  return detail::read_parquet_as_variant_helper(reader, schema,
                                                include_columns);
}
}  // namespace ygm::io