// Copyright 2019-2023 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once

#include <cassert>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <regex>
#include <string>
#include <vector>

#include <ygm/comm.hpp>
#include <ygm/detail/ygm_ptr.hpp>

#include <arrow/api.h>
#include <arrow/io/file.h>
#include <arrow/util/logging.h>

#include <parquet/api/reader.h>
#include <parquet/exception.h>
#include <parquet/metadata.h>
#include <parquet/stream_reader.h>
#include <parquet/types.h>

namespace stdfs = std::filesystem;

namespace ygm::io {

struct parquet_data_type {
  parquet::Type::type type;

  bool equal(const parquet::Type::type other_type) const {
    return other_type == type;
  }

  friend std::ostream& operator<<(std::ostream&, const parquet_data_type&);
};

std::ostream& operator<<(std::ostream& os, const parquet_data_type& t) {
  os << parquet::TypeToString(t.type);
  return os;
}

// Parquet file parser
// Only supports the plain encoding.
// Do not support nested or hierarchical columns.
class arrow_parquet_parser {
 public:
  using self_type = arrow_parquet_parser;
  // 0: a column type, 1: column name
  using file_schema_container =
      std::vector<std::tuple<parquet_data_type, std::string>>;
  using parquet_stream_reader = parquet::StreamReader;

  arrow_parquet_parser(ygm::comm& _comm) : m_comm(_comm), pthis(this) {
    pthis.check(m_comm);
  }

  arrow_parquet_parser(ygm::comm&                      _comm,
                       const std::vector<std::string>& stringpaths,
                       bool                            recursive = false)
      : m_comm(_comm), pthis(this) {
    pthis.check(m_comm);
    check_paths(stringpaths, recursive);
    read_file_schema();
    m_comm.barrier();
  }

  ~arrow_parquet_parser() { m_comm.barrier(); }

  // Returns a list of column schema information
  const file_schema_container& schema() { return m_schema; }

  const std::string& schema_to_string() { return m_schema_string; }

  template <typename Function>
  void for_all(Function fn) {
    read_files(fn);
  }

  size_t local_file_count() { return m_paths.size(); }

 private:
  /**
   * @brief Check readability of paths and iterates through directories
   *
   * @param stringpaths
   * @param recursive
   */
  void check_paths(const std::vector<std::string>& stringpaths,
                   bool                            recursive) {
    if (m_comm.rank0()) {
      std::vector<std::string> good_stringpaths;

      for (const std::string& strp : stringpaths) {
        stdfs::path p(strp);
        if (stdfs::exists(p)) {
          if (stdfs::is_regular_file(p)) {
            if (is_file_good(p)) {
              good_stringpaths.push_back(p.string());
            }
          } else if (stdfs::is_directory(p)) {
            if (recursive) {
              //
              // If a directory & user requested recursive
              const std::filesystem::recursive_directory_iterator end;
              for (std::filesystem::recursive_directory_iterator itr{p};
                   itr != end; itr++) {
                if (stdfs::is_regular_file(itr->path())) {
                  if (is_file_good(itr->path())) {
                    good_stringpaths.push_back(itr->path().string());
                  }
                }
              }  // for
            } else {
              //
              // If a directory & user requested recursive
              const std::filesystem::directory_iterator end;
              for (std::filesystem::directory_iterator itr{p}; itr != end;
                   itr++) {
                if (stdfs::is_regular_file(itr->path())) {
                  if (is_file_good(itr->path())) {
                    good_stringpaths.push_back(itr->path().string());
                  }
                }
              }  // for
            }
          }
        }
      }  // for

      //
      // Remove duplicate paths
      std::sort(good_stringpaths.begin(), good_stringpaths.end());
      good_stringpaths.erase(
          std::unique(good_stringpaths.begin(), good_stringpaths.end()),
          good_stringpaths.end());

      // Broadcast paths to all ranks
      m_comm.async_bcast(
          [](auto parquet_reader_ptr, const auto& stringpaths_vec) {
            for (const auto& stringpath : stringpaths_vec) {
              parquet_reader_ptr->m_paths.push_back(stdfs::path(stringpath));
            }
          },
          pthis, good_stringpaths);
    }

    m_comm.barrier();
  }

  /**
   * @brief Checks if file is readable
   *
   * @param p
   * @return true
   * @return false
   */
  bool is_file_good(const stdfs::path& p) {
    std::shared_ptr<arrow::io::ReadableFile> input_file;
    PARQUET_ASSIGN_OR_THROW(input_file, arrow::io::ReadableFile::Open(p));
    return true;
  }

  template <typename Function>
  void read_files(Function fn) {
    for (size_t i = 0; i < m_paths.size(); ++i) {
      if (is_owner(i)) {
        read_file(m_paths[i], fn);
      }
    }  // for
  }

  template <typename Function>
  void read_file(const stdfs::path& file_path, Function fn) {
    read_parquet_stream(file_path.string(), fn);
  }

  void read_file_schema() {
    read_parquet_stream(
        m_paths[0], [](auto& stream_reader, const auto& field_count) {}, true);
  }

  template <typename Function>
  void read_parquet_stream(std::string&& input_filename, Function fn,
                           bool read_schema_only = false) {
    // Open the Parquet file
    std::shared_ptr<arrow::io::ReadableFile> input_file;
    PARQUET_ASSIGN_OR_THROW(input_file,
                            arrow::io::ReadableFile::Open(input_filename));

    // Create a ParquetFileReader object
    std::unique_ptr<parquet::ParquetFileReader> parquet_file_reader =
        parquet::ParquetFileReader::Open(input_file);

    // Get the file schema
    parquet::SchemaDescriptor const* const file_schema =
        parquet_file_reader->metadata()->schema();

    if (read_schema_only) {
      const size_t field_count = file_schema->num_columns();
      for (size_t i = 0; i < field_count; ++i) {
        parquet::ColumnDescriptor const* const column = file_schema->Column(i);
        m_schema.emplace_back(std::forward_as_tuple(
            parquet_data_type{column->physical_type()}, column->name()));
      }  // for
      m_schema_string = file_schema->ToString();
      return;
    }

    parquet_stream_reader stream_reader{std::move(parquet_file_reader)};
    for (size_t i = 0; !stream_reader.eof(); ++i) {
      fn(stream_reader, m_schema.size());
    }  // for
  }

  bool is_owner(const size_t& item_ID) {
    return m_comm.rank() == item_ID % m_comm.size() ? true : false;
  }

  ygm::comm&                       m_comm;
  typename ygm::ygm_ptr<self_type> pthis;
  std::vector<stdfs::path>         m_paths;
  file_schema_container            m_schema;
  std::string                      m_schema_string;
};

}  // namespace ygm::io
