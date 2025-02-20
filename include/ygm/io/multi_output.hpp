// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <filesystem>
#include <fstream>
#include <memory>
#include <sstream>
#include <string>
#include <ygm/container/detail/hash_partitioner.hpp>

namespace ygm::io {

namespace fs = std::filesystem;

template <typename Partitioner =
              ygm::container::detail::old_hash_partitioner<std::string>>
class multi_output {
 public:
  using self_type = multi_output<Partitioner>;

  // filename_prefix is assumed to be a directory name and has a "/" appended if
  // not already present to force it to be a directory
  multi_output(ygm::comm &comm, std::string filename_prefix,
               size_t buffer_length = 1024 * 1024, bool append = false)
      : m_comm(comm),
        pthis(this),
        m_prefix_path(filename_prefix),
        m_buffer_length(buffer_length),
        m_append_flag(append) {
    pthis.check(m_comm);

    // Adds "/" to filename_prefix to force it to be a directory
    if (filename_prefix.back() != '/') {
      filename_prefix.append("/");
    }

    m_prefix_path = fs::path(filename_prefix);

    // Create directories to hold files
    if (comm.rank0()) {
      // Make sure prefix isn't an already existing filename
      check_prefix(m_prefix_path);

      make_directories(m_prefix_path);
    }
  }

  ~multi_output() {
    m_comm.barrier();
    flush_all_buffers();
  }

  template <typename... Args>
  void async_write_line(const std::string &subpath, Args &&...args) {
    std::string s = pack_stream(args...);

    m_comm.async(
        owner(subpath),
        [](auto mo_ptr, const std::string &subpath, const std::string &s) {
          fs::path fullname(mo_ptr->m_prefix_path);
          fullname += subpath;

          auto ofstream_iter = mo_ptr->m_map_file_pointers.find(subpath);
          if (ofstream_iter == mo_ptr->m_map_file_pointers.end()) {
            const auto [iter, success] = mo_ptr->m_map_file_pointers.insert(
                std::make_pair(subpath, /*mo_ptr->make_ofstream_ptr(fullname)*/
                               mo_ptr->make_buffered_ofstream(fullname)));
            ofstream_iter = iter;
          }

          ofstream_iter->second.buffer_output(s);
        },
        pthis, subpath, s);
  }

  ygm::comm &comm() { return m_comm; }

 private:
  class buffered_ofstream {
   public:
    buffered_ofstream(const fs::path &p, const std::ios_base::openmode mode,
                      const size_t buf_len)
        : ofstream_ptr(std::make_unique<std::ofstream>(p.c_str(), mode)),
          buffer_length(buf_len) {}

    void buffer_output(const std::string &s) {
      buffer.append(s);
      buffer.append("\n");

      if (buffer.size() > buffer_length) {
        flush_buffer();
      }
    }

    void flush_buffer() {
      if (buffer.size() == 0) return;

      ofstream_ptr->write(buffer.data(), buffer.size());
      buffer.clear();
      buffer.shrink_to_fit();
    }

   private:
    std::string                    buffer;
    std::unique_ptr<std::ofstream> ofstream_ptr;
    size_t                         buffer_length;
  };

  void flush_all_buffers() {
    for (auto &filename_buffer_pair : m_map_file_pointers) {
      filename_buffer_pair.second.flush_buffer();
    }
  }

  int owner(const std::string &subpath) {
    auto [owner, bank] = partitioner(subpath, m_comm.size(), 1024);
    return owner;
  }

  template <typename... Args>
  std::string pack_stream(Args &&...args) const {
    std::stringstream ss;
    (ss << ... << args);
    return ss.str();
  }

  void make_directories(const fs::path &p) {
    std::vector<fs::path> directory_stack;
    fs::path              curr_path = p.parent_path();

    while (!fs::exists(curr_path) && !curr_path.empty()) {
      directory_stack.push_back(curr_path);
      curr_path = curr_path.parent_path();
    }

    while (directory_stack.size() > 0) {
      fs::path &p = directory_stack.back();
      fs::create_directory(p);
      directory_stack.pop_back();
    }
  }

  buffered_ofstream make_buffered_ofstream(const fs::path &p) {
    make_directories(p);

    std::ios_base::openmode mode = std::ios::binary;
    if (m_append_flag) {
      mode |= std::ios_base::app;
    } else {
      mode |= std::ios_base::trunc;
    }

    return buffered_ofstream(p, mode, m_buffer_length);
  }

  void check_prefix(const fs::path &p) {
    std::string tmp_string = p.string();
    if (tmp_string.back() == '/') {
      tmp_string.pop_back();
    }

    fs::path tmp_path(tmp_string);

    if (fs::exists(tmp_path) && !fs::is_directory(tmp_path)) {
      m_comm.cerr() << "ERROR: Cannot use name of existing file as prefix for "
                       "ygm::io::multi_output: "
                    << tmp_path << std::endl;
      MPI_Abort(MPI_COMM_WORLD, EXIT_FAILURE);
    }
  }

  fs::path                                 m_prefix_path;
  size_t                                   m_buffer_length;
  bool                                     m_append_flag;
  std::map<std::string, buffered_ofstream> m_map_file_pointers;
  ygm::comm                               &m_comm;
  typename ygm::ygm_ptr<self_type>         pthis;
  Partitioner                              partitioner;
};
}  // namespace ygm::io
