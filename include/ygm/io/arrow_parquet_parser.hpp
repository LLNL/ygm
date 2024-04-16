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
  /// @brief Holds the information about the range of file data to be read by a
  /// rank.
  struct read_range {
    size_t begin_file_no{0};
    size_t begin_row_offset{0};
    size_t num_rows{0};

    template <typename Archive>
    void serialize(Archive& ar) {
      ar(begin_file_no, begin_row_offset, num_rows);
    }
  };

  /// @brief Count the number of lines in a file.
  static size_t count_rows(const stdfs::path& input_filename) {
    std::shared_ptr<arrow::io::ReadableFile> input_file;
    PARQUET_ASSIGN_OR_THROW(input_file,
                            arrow::io::ReadableFile::Open(input_filename));
    std::unique_ptr<parquet::ParquetFileReader> parquet_file_reader =
        parquet::ParquetFileReader::Open(input_file);

    std::shared_ptr<parquet::FileMetaData> file_metadata =
        parquet_file_reader->metadata();
    const size_t num_rows = file_metadata->num_rows();

    return num_rows;
  }

  static std::unique_ptr<parquet::ParquetFileReader> open_file(
      const stdfs::path& input_filename) {
    // Open the Parquet file
    std::shared_ptr<arrow::io::ReadableFile> input_file;
    PARQUET_ASSIGN_OR_THROW(input_file,
                            arrow::io::ReadableFile::Open(input_filename));

    // Create a ParquetFileReader object
    return parquet::ParquetFileReader::Open(input_file);
  }

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
    try {
      PARQUET_ASSIGN_OR_THROW(input_file, arrow::io::ReadableFile::Open(p));
    } catch (...) {
      return false;
    }
    return true;
  }

  template <typename Function>
  void read_files(Function fn) {
    m_comm.barrier();

    if (ARROW_VERSION_MAJOR < 14) {
      // Due to a bug in parquet::StreamReader::SkipRows() in < v14,
      // we can not read a single file using multiple ranks.
      for (size_t i = 0; i < m_paths.size(); ++i) {
        if (is_owner(i)) {
          read_parquet_stream(m_paths[i], fn);
        }
      }
    } else {
      assign_read_range();

      // If n is 0, fno and offset could contain invalid values
      ssize_t n      = ssize_t(m_read_range.num_rows);
      size_t  fno    = m_read_range.begin_file_no;
      size_t  offset = m_read_range.begin_row_offset;

      while (n > 0) {
        n -= read_parquet_stream(m_paths[fno], fn, offset, n);
        assert(n >= 0);
        offset = 0;
        ++fno;
      }
    }
  }

  void read_file_schema() {
    auto reader = open_file(m_paths[0]);

    // Get the file schema
    parquet::SchemaDescriptor const* const file_schema =
        reader->metadata()->schema();

    const size_t field_count = file_schema->num_columns();
    for (size_t i = 0; i < field_count; ++i) {
      parquet::ColumnDescriptor const* const column = file_schema->Column(i);
      m_schema.emplace_back(std::forward_as_tuple(
          parquet_data_type{column->physical_type()}, column->name()));
    }
    m_schema_string = file_schema->ToString();
  }

  /// @brief Reads a parquet file and calls a function for each row.
  /// @param input_filename Path to a parquet file.
  /// @param fn A function that takes a parquet_stream_reader and the #of
  /// @param offset #of rows to skip. This option works with >= Arrow v14.
  /// @param num_rows_to_read #of rows to read. If negative, read all rows.
  template <typename Function>
  size_t read_parquet_stream(const stdfs::path& input_filename, Function fn,
                             const size_t  offset           = 0,
                             const ssize_t num_rows_to_read = -1) {
    auto                  reader = open_file(input_filename);
    parquet_stream_reader stream{std::move(reader)};

    if (offset > 0) {
      // SkipRows() has a bug in < v14
      assert(ARROW_VERSION_MAJOR >= 14);
      stream.SkipRows(ssize_t(offset));
    }

    size_t cnt_read_rows = 0;
    while (!stream.eof()) {
      if (cnt_read_rows >= size_t(num_rows_to_read)) break;
      fn(stream, m_schema.size());
      ++cnt_read_rows;
    }

    return cnt_read_rows;
  }

  /// @brief Assigns the range each rank reads.
  /// This function tries to assign the equal number of rows to every rank.
  /// Some files may be assigned to more than one rank.
  void assign_read_range() {
    // Count the number of lines in each file
    // Can do in parallel, but use a single rank for now for simplicity.
    if (m_comm.rank0()) {
      std::vector<size_t> num_rows(m_paths.size());
      for (size_t i = 0; i < m_paths.size(); ++i) {
        num_rows[i] = count_rows(m_paths[i]);
      }

      const size_t total_num_rows =
          std::accumulate(num_rows.begin(), num_rows.end(), size_t(0));
      // std::cout << total_num_rows << std::endl; //DB

      size_t file_no           = 0;
      size_t row_no_offset     = 0;
      size_t num_assigned_rows = 0;  // For sanity check
      for (int rank_no = 0; rank_no < m_comm.size(); ++rank_no) {
        size_t per_rank_num_rows = total_num_rows / m_comm.size();
        if (rank_no < (total_num_rows % m_comm.size())) {
          per_rank_num_rows += 1;
        }

        read_range range;
        range.begin_file_no    = file_no;
        range.begin_row_offset = row_no_offset;
        range.num_rows         = 0;

        for (; file_no < m_paths.size(); ++file_no) {
          // #of rows to read from this file
          const size_t n = std::min(per_rank_num_rows - range.num_rows,
                                    num_rows[file_no] - row_no_offset);
          range.num_rows += n;
          if (range.num_rows < per_rank_num_rows) {
            // Can read more rows from the next file
            row_no_offset = 0;
          } else {
            // Found the enough rows to read
            row_no_offset += n;
            break;
          }
        }
        num_assigned_rows += range.num_rows;

        // Send the read range to the rank
        m_comm.async(
            rank_no,
            [](auto parquet_reader_ptr, const auto& range) {
              parquet_reader_ptr->m_read_range = range;
            },
            pthis, range);
      }

      // Sanity check
      assert(num_assigned_rows == total_num_rows);
    }
    m_comm.barrier();
  }

  bool is_owner(const size_t& item_ID) {
    return m_comm.rank() == item_ID % m_comm.size() ? true : false;
  }

  ygm::comm&                       m_comm;
  typename ygm::ygm_ptr<self_type> pthis;
  std::vector<stdfs::path>         m_paths;
  file_schema_container            m_schema;
  std::string                      m_schema_string;
  read_range                       m_read_range;
};

}  // namespace ygm::io
