// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once

#include <filesystem>
#include <fstream>
#include <string>
#include <vector>
#include <ygm/container/detail/base_iteration.hpp>

namespace ygm::io {

namespace fs = std::filesystem;

/**
 * @brief Distributed text file parsing.
 *
 */
class line_parser : public ygm::container::detail::base_iteration_value<
                        line_parser, std::tuple<std::string>> {
 public:
  using for_all_args = std::tuple<std::string>;
  /**
   * @brief Construct a new line parser object
   *
   * @param comm
   * @param stringpaths
   * @param node_local_filesystem True if paths are to a node-local filesystem
   * @param recursive True if directory traversal should be recursive
   */
  line_parser(ygm::comm& comm, const std::vector<std::string>& stringpaths,
              bool node_local_filesystem = false, bool recursive = false)
      : m_comm(comm),
        m_node_local_filesystem(node_local_filesystem),
        m_skip_first_line(false) {
    if (node_local_filesystem) {
      YGM_ASSERT_RELEASE(false);
      check_paths(stringpaths, recursive);
    } else {
      if (m_comm.rank0()) {
        check_paths(stringpaths, recursive);
      }
    }
  }

  /**
   * @brief Executes a user function for every line in a set of files.
   *
   * @tparam Function
   * @param fn User function to execute
   */
  template <typename Function>
  void for_all(Function fn) {
    if (m_node_local_filesystem) {
      YGM_ASSERT_RELEASE(false);
      if (m_paths.empty()) return;
    } else {
      static std::vector<std::tuple<fs::path, size_t, size_t>> my_file_paths;

      //
      //  Splits files over ranks by file size.   8MB is smallest granularity.
      //  This approach could be improved by having rank_layout information.
      m_comm.barrier();
      if (m_comm.rank0()) {
        std::vector<std::tuple<fs::path, size_t, size_t>> remaining_files(
            m_paths.size());
        size_t total_size{0};
        for (size_t i = 0; i < m_paths.size(); ++i) {
          size_t fsize = fs::file_size(m_paths[i]);
          total_size += fsize;
          remaining_files[i] = std::make_tuple(m_paths[i], size_t(0), fsize);
        }

        if (total_size > 0) {
          size_t bytes_per_rank = std::max((total_size / m_comm.size()) + 1,
                                           size_t(8 * 1024 * 1024));
          for (int rank = 0; rank < m_comm.size(); ++rank) {
            size_t remaining_budget = bytes_per_rank;
            while (remaining_budget > 0 && !remaining_files.empty()) {
              size_t file_remaining = std::get<2>(remaining_files.back()) -
                                      std::get<1>(remaining_files.back());
              size_t& cur_position = std::get<1>(remaining_files.back());
              if (file_remaining > remaining_budget) {
                m_comm.async(
                    rank,
                    [](const std::string& fname, size_t bytes_begin,
                       size_t bytes_end) {
                      my_file_paths.push_back(
                          {fs::path(fname), bytes_begin, bytes_end});
                    },
                    (std::string)std::get<0>(remaining_files.back()),
                    cur_position, cur_position + remaining_budget);
                cur_position += remaining_budget;
                remaining_budget = 0;
              } else if (file_remaining <= remaining_budget) {
                m_comm.async(
                    rank,
                    [](const std::string& fname, size_t bytes_begin,
                       size_t bytes_end) {
                      my_file_paths.push_back(
                          {fs::path(fname), bytes_begin, bytes_end});
                    },
                    (std::string)std::get<0>(remaining_files.back()),
                    cur_position, std::get<2>(remaining_files.back()));
                remaining_budget -= file_remaining;
                remaining_files.pop_back();
              }
            }
          }
        }
      }
      m_comm.barrier();

      //
      // Each rank process locally assigned files.
      for (const auto& fname : my_file_paths) {
        // m_comm.cout("Opening: ", std::get<0>(fname), " ", std::get<1>(fname),
        //             " ", std::get<2>(fname));
        std::ifstream ifs(std::get<0>(fname));
        // Note: Current process is responsible for reading up to *AND
        // INCLUDING* bytes_end
        size_t bytes_begin = std::get<1>(fname);
        size_t bytes_end   = std::get<2>(fname);
        YGM_ASSERT_RELEASE(ifs.good());
        ifs.imbue(std::locale::classic());
        std::string line;
        bool        first_line = false;
        // Throw away line containing bytes_begin as it was read by the previous
        // process (unless it corresponds to the beginning of a file)
        if (bytes_begin > 0) {
          ifs.seekg(bytes_begin);
          std::getline(ifs, line);
        } else {
          first_line = true;
        }
        // Keep reading until line containing bytes_end is read
        while (ifs.tellg() <= bytes_end && std::getline(ifs, line)) {
          // Skip first line if necessary
          if (not first_line || not m_skip_first_line) {
            fn(line);
          } else {
          }
          // if(ifs.tellg() > bytes_end) break;
          first_line = false;
        }
      }
      my_file_paths.clear();
    }
  }

  std::string read_first_line() {
    std::string line;
    if (m_comm.rank0()) {
      std::ifstream ifs(m_paths[0]);
      std::getline(ifs, line);
    }

    line = m_comm.mpi_bcast(line, 0, m_comm.get_mpi_comm());

    return line;
  }

  void set_skip_first_line(bool skip_first) { m_skip_first_line = skip_first; }

 private:
  /**
   * @brief Check readability of paths and iterates through directories
   *
   * @param stringpaths
   * @param recursive
   */
  void check_paths(const std::vector<std::string>& stringpaths,
                   bool                            recursive) {
    //
    //
    for (const std::string& strp : stringpaths) {
      fs::path p(strp);
      if (fs::exists(p)) {
        if (fs::is_regular_file(p)) {
          if (is_file_good(p)) {
            m_paths.push_back(p);
          }
        } else if (fs::is_directory(p)) {
          if (recursive) {
            //
            // If a directory & user requested recursive
            const std::filesystem::recursive_directory_iterator end;
            for (std::filesystem::recursive_directory_iterator itr{p};
                 itr != end; itr++) {
              if (fs::is_regular_file(itr->path())) {
                if (is_file_good(itr->path())) {
                  m_paths.push_back(itr->path());
                }
              }
            }
          } else {
            //
            // If a directory & user requested recursive
            const std::filesystem::directory_iterator end;
            for (std::filesystem::directory_iterator itr{p}; itr != end;
                 itr++) {
              if (fs::is_regular_file(itr->path())) {
                if (is_file_good(itr->path())) {
                  m_paths.push_back(itr->path());
                }
              }
            }
          }
        }
      }
    }

    //
    // Remove duplicate paths
    std::sort(m_paths.begin(), m_paths.end());
    m_paths.erase(std::unique(m_paths.begin(), m_paths.end()), m_paths.end());
  }

  /**
   * @brief Checks if file is readable
   *
   * @param p
   * @return true
   * @return false
   */
  bool is_file_good(const fs::path& p) {
    std::ifstream ifs(p);
    bool          good = ifs.good();
    if (!good) {
      m_comm.cout("WARNING: unable to open: ", p);
    }
    return good;
  }
  ygm::comm&            m_comm;
  std::vector<fs::path> m_paths;
  bool                  m_node_local_filesystem;
  bool                  m_skip_first_line;
};

}  // namespace ygm::io
