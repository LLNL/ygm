// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once

#include <fstream>
#include <memory>
#include <sstream>
#include <string>
#include <ygm/container/detail/hash_partitioner.hpp>

namespace ygm::io {

namespace fs = std::filesystem;

template <typename Partitioner =
              ygm::container::detail::hash_partitioner<std::string>>
class multi_output {
 public:
  using self_type = multi_output<Partitioner>;

  multi_output(ygm::comm &comm, const std::string &filename_prefix,
               bool append = false)
      : m_comm(comm),
        pthis(this),
        m_prefix_path(filename_prefix),
        m_append_flag(append) {
    pthis.check(m_comm);

    // Create directories to hold files
    if (comm.rank0()) {
      // Make sure prefix isn't an already existing filename
      check_prefix(m_prefix_path);

      make_directories(m_prefix_path);
    }
  }

  ~multi_output() { m_comm.barrier(); }

  template <typename... Args>
  void async_write_line(const std::string &subpath, Args &&... args) {
    std::string s = pack_stream(args...);

    m_comm.async(
        owner(subpath),
        [](auto mo_ptr, const std::string &subpath, const std::string &s) {
          fs::path fullname(mo_ptr->m_prefix_path);
          fullname += subpath;

          auto ofstream_iter = mo_ptr->m_map_file_pointers.find(subpath);
          if (ofstream_iter == mo_ptr->m_map_file_pointers.end()) {
            const auto [iter, success] = mo_ptr->m_map_file_pointers.insert(
                std::make_pair(subpath, mo_ptr->make_ofstream_ptr(fullname)));
            ofstream_iter = iter;
          }

          *(ofstream_iter->second) << s << "\n";
        },
        pthis, subpath, s);
  }

  ygm::comm &comm() { return m_comm; }

 private:
  int owner(const std::string &subpath) {
    auto [owner, bank] = partitioner(subpath, m_comm.size(), 1024);
    return owner;
  }

  template <typename... Args>
  std::string pack_stream(Args &&... args) const {
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

  std::unique_ptr<std::ofstream> make_ofstream_ptr(const fs::path &p) {
    make_directories(p);

    std::ios_base::openmode mode = std::ios_base::out;
    if (m_append_flag) {
      mode |= std::ios_base::app;
    } else {
      mode |= std::ios_base::trunc;
    }

    return std::make_unique<std::ofstream>(p.c_str(), mode);
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

  fs::path                                              m_prefix_path;
  bool                                                  m_append_flag;
  std::map<std::string, std::unique_ptr<std::ofstream>> m_map_file_pointers;
  ygm::comm                                             m_comm;
  typename ygm::ygm_ptr<self_type>                      pthis;
  Partitioner                                           partitioner;
};
}  // namespace ygm::io
