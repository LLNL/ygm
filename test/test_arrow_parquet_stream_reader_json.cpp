// Copyright 2019-2022 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#undef NDEBUG

#include <filesystem>
#include <ygm/comm.hpp>
#include <ygm/detail/cereal_boost_json.hpp>
#include <ygm/io/arrow_parquet_parser.hpp>
#include <ygm/io/detail/arrow_parquet_json_converter.hpp>

int main(int argc, char** argv) {
  ygm::comm world(&argc, &argv);

  const std::string dir_name = "data/parquet_files_json/";

  ygm::io::arrow_parquet_parser parquetp(world, {dir_name});

  static size_t cnt1 = 0;
  static size_t cnt2 = 0;
  static size_t cnt3 = 0;

  // Test if ygm::io::detail::read_parquet_as_json can read all supported data
  // types correctly
  const auto& schema = parquetp.schema();
  parquetp.for_all([&schema, &world](auto& stream_reader, const auto&) {
    const auto obj =
        ygm::io::detail::read_parquet_as_json(stream_reader, schema);

    world.async(
        0,
        [](auto, const auto& obj) {
          ASSERT_RELEASE(obj.contains("id"));
          ASSERT_RELEASE(obj.contains("bool"));
          ASSERT_RELEASE(obj.contains("int32"));
          ASSERT_RELEASE(obj.contains("int64"));
          ASSERT_RELEASE(obj.contains("float"));
          ASSERT_RELEASE(obj.contains("double"));
          ASSERT_RELEASE(obj.contains("byte_array"));

          ASSERT_RELEASE(obj.at("id").is_int64());
          ASSERT_RELEASE(obj.at("bool").is_bool());
          ASSERT_RELEASE(obj.at("int32").is_int64());
          ASSERT_RELEASE(obj.at("int64").is_int64());
          ASSERT_RELEASE(obj.at("float").is_double());
          ASSERT_RELEASE(obj.at("double").is_double());
          ASSERT_RELEASE(obj.at("byte_array").is_string());

          const auto id = obj.at("id").as_int64();
          if (id == 0) {
            ASSERT_RELEASE(obj.at("bool").as_bool() == true);
            ASSERT_RELEASE(obj.at("int32").as_int64() == -1);
            ASSERT_RELEASE(obj.at("int64").as_int64() == -(1ULL << 32) - 1);
            ASSERT_RELEASE(obj.at("float").as_double() == 1.5);
            ASSERT_RELEASE(obj.at("double").as_double() == 10.5);
            ASSERT_RELEASE(obj.at("byte_array").as_string() == "aa");
            ++cnt1;
          } else if (id == 1) {
            ASSERT_RELEASE(obj.at("bool").as_bool() == false);
            ASSERT_RELEASE(obj.at("int32").as_int64() == -2);
            ASSERT_RELEASE(obj.at("int64").as_int64() == -(1ULL << 32) - 2);
            ASSERT_RELEASE(obj.at("float").as_double() == 2.5);
            ASSERT_RELEASE(obj.at("double").as_double() == 20.5);
            ASSERT_RELEASE(obj.at("byte_array").as_string() == "bb");
            ++cnt2;
          } else if (id == 2) {
            ASSERT_RELEASE(obj.at("bool").as_bool() == true);
            ASSERT_RELEASE(obj.at("int32").as_int64() == -3);
            ASSERT_RELEASE(obj.at("int64").as_int64() == -(1ULL << 32) - 3);
            ASSERT_RELEASE(obj.at("float").as_double() == 3.5);
            ASSERT_RELEASE(obj.at("double").as_double() == 30.5);
            ASSERT_RELEASE(obj.at("byte_array").as_string() == "cc");
            ++cnt3;
          } else {
            ASSERT_RELEASE(false);
          }
        },
        obj);
  });
  world.barrier();

  if (world.rank0()) {
    ASSERT_RELEASE(cnt1 == 1);
    ASSERT_RELEASE(cnt2 == 1);
    ASSERT_RELEASE(cnt3 == 1);
  } else {
    ASSERT_RELEASE(cnt1 == 0);
    ASSERT_RELEASE(cnt2 == 0);
    ASSERT_RELEASE(cnt3 == 0);
  }

  return 0;
}
