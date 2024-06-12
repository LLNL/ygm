// Copyright 2019-2022 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <iostream>
#include <string>
#include <vector>

#include <ygm/comm.hpp>
#include <ygm/io/parquet_parser.hpp>

namespace stdfs = std::filesystem;

int main(int argc, char** argv) {
  ygm::comm world(&argc, &argv);

  world.cout0() << "Arrow Parquet file parser example" << std::endl;

  // assuming the build directory is inside the YGM root directory
  const std::string dir_name = "../test/data/parquet_files/";

  // parquet_parser assumes files have identical scehma
  ygm::io::parquet_parser parquetp(world, {dir_name});

  world.cout0() << parquetp.file_count() << " files in " << dir_name
                << std::endl;
  world.cout0() << "#Fields: " << parquetp.schema().size() << std::endl;
  world.cout0() << "Schema: " << std::endl;
  for (size_t i = 0; i < parquetp.schema().size(); ++i) {
    world.cout0() << std::get<0>(parquetp.schema()[i]) << ":"
                  << std::get<1>(parquetp.schema()[i]) << ", ";
  }
  world.cout0() << std::endl;
  world.cout0() << parquetp.schema_to_string() << std::endl;

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
  world.cout0() << "#Rows: " << row_count << std::endl;

  // read fields in each row

  struct columns {
    std::string string_field;
    char        char_array_field[4];
    uint64_t    uint64_t_field;
    double      double_field;
    bool        boolean_field;
  };

  std::vector<columns> rows;

  parquetp.for_all([&rows](auto& stream_reader, const auto& field_count) {
    using columns_t = decltype(rows)::value_type;
    columns_t columns_obj;
    stream_reader >> columns_obj.string_field;
    stream_reader >> columns_obj.char_array_field;
    stream_reader >> columns_obj.uint64_t_field;
    stream_reader >> columns_obj.double_field;
    stream_reader >> columns_obj.boolean_field;
    stream_reader.EndRow();
    rows.emplace_back(columns_obj);
  });

  world.barrier();

  auto row_count_2 = world.all_reduce_sum(rows.size());
  assert(row_count == row_count_2);

  for (size_t i = 0; i < parquetp.schema().size(); ++i) {
    world.cout0() << "(" << std::get<1>(parquetp.schema()[i]) << ") ";
  }
  world.cout0() << std::endl;

  world.barrier();

  for (size_t i = 0; i < std::min((size_t)3, rows.size()); ++i) {
    auto& obj_ref = rows[i];
    world.cout() << std::boolalpha << obj_ref.string_field << ", "
                 << obj_ref.char_array_field << ", " << obj_ref.uint64_t_field
                 << ", " << obj_ref.double_field << ", "
                 << obj_ref.boolean_field << std::endl;
  }

  world.barrier();

  return 0;
}
