// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#undef NDEBUG

#include <filesystem>
#include <ygm/comm.hpp>
#include <ygm/io/daily_output.hpp>
#include <ygm/io/line_parser.hpp>

namespace fs = std::filesystem;

int main(int argc, char **argv) {
  ygm::comm world(&argc, &argv);

  // Check files created
  {
    std::string base_dir{"test_dir/"};
    std::string prefix_path{base_dir + std::string("nested_dir/")};

    {
      ygm::io::daily_output d(world, prefix_path);

      std::string message("my message from rank " +
                          std::to_string(world.rank()));

      d.async_write_line(0, message);
    }

    if (world.rank0()) {
      std::string expected_path(prefix_path + "1970/1/1");
      YGM_ASSERT_RELEASE(fs::exists(fs::path(expected_path)));
      fs::remove_all(fs::path(base_dir));
    }
  }

  // Test writing
  {
    uint64_t    xor_write;
    uint64_t    xor_read{0};
    std::string base_dir{"test_dir/"};
    std::string prefix_path{base_dir + std::string("nested_dir/")};

    // Write lines to file
    {
      ygm::io::daily_output d(world, prefix_path);

      // Each rank writes to different month
      uint64_t    timestamp{2678400 * ((uint64_t)world.rank())};
      std::string message("my message from rank " +
                          std::to_string(world.rank()));

      xor_write = std::hash<std::string>()(message);
      d.async_write_line(timestamp, message);

      xor_write = world.all_reduce(
          xor_write, [](const uint64_t a, const uint64_t b) { return a ^ b; });
    }

    // Read lines back
    {
      ygm::io::line_parser linep(world, std::vector<std::string>{prefix_path},
                                 false, true);

      linep.for_all([&xor_read](const std::string &line) {
        xor_read ^= std::hash<std::string>()(line);
      });

      xor_read = world.all_reduce(
          xor_read, [](const uint64_t a, const uint64_t b) { return a ^ b; });
    }

    // Clean up files
    if (world.rank0()) {
      fs::remove_all(fs::path(base_dir));
    }

    YGM_ASSERT_RELEASE(xor_write == xor_read);
  }

  // Test appending
  {
    uint64_t    xor_write;
    uint64_t    xor_read{0};
    std::string base_dir{"test_dir/"};
    std::string prefix_path{base_dir + std::string("nested_dir/")};

    // Write lines to file
    {
      ygm::io::daily_output d(world, prefix_path, 1024, false);

      uint64_t    timestamp{2678400 * ((uint64_t)world.rank())};
      std::string message("my message from rank " +
                          std::to_string(world.rank()));

      xor_write = std::hash<std::string>()(message);
      d.async_write_line(timestamp, message);
    }

    // Append new lines
    {
      ygm::io::daily_output d(world, prefix_path, 1024, true);

      uint64_t    timestamp{2678400 * ((uint64_t)world.rank())};
      std::string message("my second message from rank " +
                          std::to_string(world.rank()));

      xor_write ^= std::hash<std::string>()(message);
      d.async_write_line(timestamp, message);

      xor_write = world.all_reduce(
          xor_write, [](const uint64_t a, const uint64_t b) { return a ^ b; });
    }

    // Read lines back
    {
      ygm::io::line_parser linep(world, std::vector<std::string>{prefix_path},
                                 false, true);

      linep.for_all([&xor_read](const std::string &line) {
        xor_read ^= std::hash<std::string>()(line);
      });

      xor_read = world.all_reduce(
          xor_read, [](const uint64_t a, const uint64_t b) { return a ^ b; });
    }

    // Clean up files
    if (world.rank0()) {
      fs::remove_all(fs::path(base_dir));
    }

    YGM_ASSERT_RELEASE(xor_write == xor_read);
  }

  return 0;
}
