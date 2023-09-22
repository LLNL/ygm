// Copyright 2019-2023 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <iostream>
#include <string>
#include <vector>

#include <boost/json/src.hpp>

#include <ygm/comm.hpp>
#include <ygm/detail/cereal_boost_json.hpp>
#include <ygm/io/detail/arrow_parquet_json_converter.hpp>

int main(int argc, char** argv) {
  ygm::comm world(&argc, &argv);

  world.cout0()
      << "Arrow Parquet file parser example (reads data as JSON objects)"
      << std::endl;

  // assuming the build directory is inside the YGM root directory
  const std::string dir_name = "../test/data/parquet_files2/";

  ygm::io::arrow_parquet_parser parquetp(world, {dir_name});

  const auto& schema = parquetp.schema();

  parquetp.for_all([&schema, &world](auto& stream_reader, const auto&) {
    // obj's type is boost::json::object
    const auto obj =
        ygm::io::detail::read_parquet_as_json(stream_reader, schema);

    world.async(
        0, [](auto, const auto& obj) { std::cout << obj << std::endl; }, obj);
  });

  return 0;
}
