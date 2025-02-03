// Copyright 2019-2023 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

// Usage:
// cd /ygm/build/dir
// mpirun -np 2 ./parquet_stream_reader_json \
//  [(option) /path/to/parquet/file/or/dir]

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <iostream>
#include <string>
#include <vector>

#include <boost/json/src.hpp>

#include <ygm/comm.hpp>
#include <ygm/detail/cereal_boost_json.hpp>
#include <ygm/io/parquet2json.hpp>

int main(int argc, char** argv) {
  ygm::comm world(&argc, &argv);

  world.cout0()
      << "Arrow Parquet file parser example (reads data as JSON objects)"
      << std::endl;

  // assuming the build directory is inside the YGM root directory
  std::string dir_name = "../test/data/parquet_files_json/";
  if (argc == 2) {
    dir_name = argv[1];
  }

  ygm::io::parquet_parser parquetp(world, {dir_name});

  world.cout0() << "Schema:\n" << parquetp.schema_to_string() << std::endl;

  world.cout0() << "Read data as JSON:" << std::endl;
  const auto& schema = parquetp.schema();
  parquetp.for_all([&schema, &world](auto& stream_reader, const auto&) {
    // obj's type is boost::json::object
    const auto obj = ygm::io::read_parquet_as_json(stream_reader, schema);

    world.async(
        0, [](auto, const auto& obj) { std::cout << obj << std::endl; }, obj);
  });

  return 0;
}
