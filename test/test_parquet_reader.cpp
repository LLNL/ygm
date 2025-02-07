// Copyright 2019-2022 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#undef NDEBUG

#include <filesystem>
#include <ygm/comm.hpp>
#include <ygm/io/parquet_parser.hpp>

int main(int argc, char** argv) {
  ygm::comm world(&argc, &argv);

  //
  // Test number of lines in files
  {
    // assuming the build directory is inside the YGM root directory
    const std::string dir_name = "data/parquet_files/";

    // parquet_parser assumes files have identical scehma
    ygm::io::parquet_parser parquetp(world, {dir_name});

    // count total number of rows in files
    size_t local_count = 0;

    parquetp.for_all(
        [&local_count](auto& stream_reader, const auto& field_count) {
          stream_reader.SkipColumns(field_count);
          stream_reader.EndRow();
          local_count++;
        });

    world.barrier();
    auto row_count = world.all_reduce_sum(local_count);
    YGM_ASSERT_RELEASE(row_count == 12);
  }

  //
  // Test table entries
  {
    // assuming the build directory is inside the YGM root directory
    const std::string dir_name = "data/parquet_files/";

    // parquet_parser assumes files have identical scehma
    ygm::io::parquet_parser parquetp(world, {dir_name});

    // read fields in each row
    struct columns {
      std::string string_field;
      char        char_array_field[4];
      uint64_t    uint64_t_field;
      double      double_field;
      bool        boolean_field;
    };

    std::vector<columns>  rows;
    std::set<std::string> strings;

    parquetp.for_all(
        [&rows, &strings](auto& stream_reader, const auto& field_count) {
          using columns_t = decltype(rows)::value_type;
          columns_t columns_obj;
          stream_reader >> columns_obj.string_field;
          stream_reader >> columns_obj.char_array_field;
          stream_reader >> columns_obj.uint64_t_field;
          stream_reader >> columns_obj.double_field;
          stream_reader >> columns_obj.boolean_field;
          stream_reader.EndRow();
          rows.emplace_back(columns_obj);

          strings.insert(columns_obj.string_field);
        });

    world.barrier();
    auto row_count = world.all_reduce_sum(rows.size());
    YGM_ASSERT_RELEASE(row_count == 12);

    YGM_ASSERT_RELEASE(
        world.all_reduce_sum(strings.count("Hennessey Venom F5")) == 1);
  }

  //
  // Test the parallel read using files that contain different number of rows
  {
    // assuming the build directory is inside the YGM root directory
    const std::filesystem::path dir_name =
        "data/parquet_files_different_sizes/";

    // This test case tests the following cases (assuming there are 4
    // processes, and Arrow >= v14):
    // 1. 0 item files at the top and end.
    // 2. read a large file by multiple processes.
    // 3. a small file is read by a single process.
    // 4. a single process reads multiple files.
    // 5. skip files that contain nothing
    // 6. total number of rows does not have to
    // be splitable evenly by all processes.
    //
    // Every file contains 1 column, and thre are 11 items in total.
    // n-th item's value is 10^n, thus the sum of all value is 11,111,111,111.
    ygm::io::parquet_parser parquetp(world, {dir_name / "0.parquet",  // 0 item
                                             dir_name / "1.parquet",  // 7 items
                                             dir_name / "2.parquet",  // 0 item
                                             dir_name / "3.parquet",  // 0 item
                                             dir_name / "4.parquet",  // 2 items
                                             dir_name / "5.parquet",  // 1 item
                                             dir_name / "6.parquet",  // 1 item
                                             dir_name / "7.parquet"}  // 0 item
    );

    // count total number of rows in the files
    size_t  local_count = 0;
    int64_t local_sum   = 0;
    parquetp.for_all([&local_sum, &local_count](auto&       stream_reader,
                                                const auto& field_count) {
      if (field_count > 0) {
        int64_t buf;
        stream_reader >> buf;
        local_sum += buf;
      }
      stream_reader.SkipColumns(field_count);
      stream_reader.EndRow();
      local_count++;
    });

    world.barrier();
    const auto sum = world.all_reduce_sum(local_sum);
    YGM_ASSERT_RELEASE(sum == 11111111111);
    const auto row_count = world.all_reduce_sum(local_count);
    YGM_ASSERT_RELEASE(row_count == 11);
  }

  return 0;
}
