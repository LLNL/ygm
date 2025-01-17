// Copyright 2019-2024 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

// Usage:
// cd /ygm/build/dir
// mpirun -np 2 ./parquet_stream_reader_variant \
//  [(option) /path/to/parquet/file/or/dir]

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <iostream>
#include <string>
#include <variant>
#include <vector>

#include <ygm/comm.hpp>
#include <ygm/io/parquet2variant.hpp>

int main(int argc, char** argv) {
  ygm::comm world(&argc, &argv);

  world.cout0() << "Arrow Parquet file parser example (reads data as "
                   "std::variant objects)"
                << std::endl;

  // assuming the build directory is inside the YGM root directory
  std::string dir_name = "../test/data/parquet_files_json/";
  if (argc == 2) {
    dir_name = argv[1];
  }

  ygm::io::parquet_parser parquetp(world, {dir_name});

  const auto& schema = parquetp.schema();

  // Print column name
  world.cout0() << "Column names:" << std::endl;
  for (size_t i = 0; i < schema.size(); ++i) {
    world.cout0() << "[" << std::get<1>(schema[i]) << "]";
    if (i < schema.size() - 1) {
      world.cout0() << "\t";
    }
  }
  world.cout0() << std::endl;

  world.cout0() << "Read data as variants:" << std::endl;
  std::size_t num_rows     = 0;
  std::size_t num_valids   = 0;
  std::size_t num_invalids = 0;
  parquetp.for_all([&schema, &num_valids, &num_invalids, &num_rows](
                       auto& stream_reader, const auto&) {
    const std::vector<ygm::io::parquet_type_variant> row =
        ygm::io::read_parquet_as_variant(stream_reader, schema);
    ++num_rows;
    for (const auto& field : row) {
      if (std::holds_alternative<std::monostate>(field)) {
        ++num_invalids;
      } else {
        ++num_valids;
      }
    }
  });

  world.cout0() << "#of rows = " << world.all_reduce_sum(num_rows) << std::endl;
  world.cout0() << "#of valid items = " << world.all_reduce_sum(num_valids)
                << std::endl;
  world.cout0() << "#of invalid items = " << world.all_reduce_sum(num_invalids)
                << std::endl;

  return 0;
}
