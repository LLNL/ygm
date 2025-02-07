// Copyright 2019-2022 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#undef NDEBUG

#include <filesystem>
#include <ygm/comm.hpp>
#include <ygm/detail/cereal_boost_json.hpp>
#include <ygm/io/parquet2json.hpp>
#include <ygm/io/parquet_parser.hpp>

int main(int argc, char** argv) {
  ygm::comm world(&argc, &argv);

  const std::string dir_name = "data/parquet_files_json/";

  ygm::io::parquet_parser parquetp(world, {dir_name});

  static size_t cnt1 = 0;
  static size_t cnt2 = 0;
  static size_t cnt3 = 0;

  // Test if ygm::io::read_parquet_as_json can read all supported data
  // types correctly
  const auto& schema = parquetp.schema();
  parquetp.for_all([&schema, &world](auto& stream_reader, const auto&) {
    const auto obj = ygm::io::read_parquet_as_json(stream_reader, schema);

    world.async(
        0,
        [](auto, const auto& obj) {
          YGM_ASSERT_RELEASE(obj.contains("id"));
          YGM_ASSERT_RELEASE(obj.contains("bool"));
          YGM_ASSERT_RELEASE(obj.contains("int32"));
          YGM_ASSERT_RELEASE(obj.contains("int64"));
          YGM_ASSERT_RELEASE(obj.contains("float"));
          YGM_ASSERT_RELEASE(obj.contains("double"));
          YGM_ASSERT_RELEASE(obj.contains("byte_array"));

          YGM_ASSERT_RELEASE(obj.at("id").is_int64());
          YGM_ASSERT_RELEASE(obj.at("bool").is_bool());
          YGM_ASSERT_RELEASE(obj.at("int32").is_int64());
          YGM_ASSERT_RELEASE(obj.at("int64").is_int64());
          YGM_ASSERT_RELEASE(obj.at("float").is_double());
          YGM_ASSERT_RELEASE(obj.at("double").is_double());
          YGM_ASSERT_RELEASE(obj.at("byte_array").is_string());

          const auto id = obj.at("id").as_int64();
          if (id == 0) {
            YGM_ASSERT_RELEASE(obj.at("bool").as_bool() == true);
            YGM_ASSERT_RELEASE(obj.at("int32").as_int64() == -1);
            YGM_ASSERT_RELEASE(obj.at("int64").as_int64() == -(1ULL << 32) - 1);
            YGM_ASSERT_RELEASE(obj.at("float").as_double() == 1.5);
            YGM_ASSERT_RELEASE(obj.at("double").as_double() == 10.5);
            YGM_ASSERT_RELEASE(obj.at("byte_array").as_string() == "aa");
            ++cnt1;
          } else if (id == 1) {
            YGM_ASSERT_RELEASE(obj.at("bool").as_bool() == false);
            YGM_ASSERT_RELEASE(obj.at("int32").as_int64() == -2);
            YGM_ASSERT_RELEASE(obj.at("int64").as_int64() == -(1ULL << 32) - 2);
            YGM_ASSERT_RELEASE(obj.at("float").as_double() == 2.5);
            YGM_ASSERT_RELEASE(obj.at("double").as_double() == 20.5);
            YGM_ASSERT_RELEASE(obj.at("byte_array").as_string() == "bb");
            ++cnt2;
          } else if (id == 2) {
            YGM_ASSERT_RELEASE(obj.at("bool").as_bool() == true);
            YGM_ASSERT_RELEASE(obj.at("int32").as_int64() == -3);
            YGM_ASSERT_RELEASE(obj.at("int64").as_int64() == -(1ULL << 32) - 3);
            YGM_ASSERT_RELEASE(obj.at("float").as_double() == 3.5);
            YGM_ASSERT_RELEASE(obj.at("double").as_double() == 30.5);
            YGM_ASSERT_RELEASE(obj.at("byte_array").as_string() == "cc");
            ++cnt3;
          } else {
            YGM_ASSERT_RELEASE(false);
          }
        },
        obj);
  });
  world.barrier();

  if (world.rank0()) {
    YGM_ASSERT_RELEASE(cnt1 == 1);
    YGM_ASSERT_RELEASE(cnt2 == 1);
    YGM_ASSERT_RELEASE(cnt3 == 1);
  } else {
    YGM_ASSERT_RELEASE(cnt1 == 0);
    YGM_ASSERT_RELEASE(cnt2 == 0);
    YGM_ASSERT_RELEASE(cnt3 == 0);
  }

  return 0;
}
