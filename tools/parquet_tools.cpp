// Copyright 2019-2024 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#include <unistd.h>
#include <algorithm>
#include <cassert>
#include <cstdint>
#include <iostream>
#include <string>
#include <vector>

#include <ygm/comm.hpp>
#include <ygm/io/arrow_parquet_parser.hpp>
#include <ygm/io/detail/arrow_parquet_json_converter.hpp>
#include <ygm/io/detail/arrow_parquet_variant_converter.hpp>
#include <ygm/utility.hpp>

// #define NDEBUG

namespace stdfs = std::filesystem;

struct options_t {
  std::string subcommand;
  std::string input_path;
  bool        variant    = false;
  bool        json       = false;
  bool        read_lines = false;
};

bool parse_arguments(int argc, char** argv, options_t&, bool&);
template <typename os_t>
void show_usage(os_t&);
void count_rows(const options_t&, ygm::comm&);

int main(int argc, char** argv) {
  ygm::comm world(&argc, &argv);
  {
    options_t opt;
    bool      show_help = false;
    if (!parse_arguments(argc, argv, opt, show_help)) {
      world.cerr0() << "Invalid arguments." << std::endl;
      show_usage(world.cerr0());
    }
    if (show_help) {
      show_usage(world.cout0());
      return 0;
    }

    if (opt.subcommand == "rowcount") {
      count_rows(opt, world);
    } else if (opt.subcommand == "schema") {
      world.cout0() << "Schema" << std::endl;
      ygm::io::arrow_parquet_parser parquetp(world, {opt.input_path.c_str()});
      world.cout0() << parquetp.schema_to_string() << std::endl;
    } else {
      world.cerr0() << "Unknown subcommand: " << opt.subcommand << std::endl;
    }
  }
  world.barrier();

  return 0;
}

bool parse_arguments(int argc, char** argv, options_t& options,
                     bool& show_help) {
  int opt;
  while ((opt = getopt(argc, argv, "c:p:vjlh")) != -1) {
    switch (opt) {
      case 'c':
        options.subcommand = optarg;
        break;
      case 'p':
        options.input_path = optarg;
        break;
      case 'v':
        options.read_lines = true;
        options.variant    = true;
        break;
      case 'j':
        options.read_lines = true;
        options.json       = true;
        break;
      case 'l':
        options.read_lines = true;
        break;
      case 'h':
        show_help = true;
        break;
      default:
        return false;
    }
  }
  return true;
}

template <typename os_t>
void show_usage(os_t& os) {
  os << "Usage: parquet-tools -c <subcommand> -p <input file or dir path> "
        "[subcommand options]"
     << std::endl;
  os << "-c <subcommand>:" << std::endl;
  os << "  rowcount" << std::endl;
  os << "    Return the number of rows in parquet files. If no subcommand "
        "option was specified, return the value stored in the metadata."
     << std::endl;
  os << "  schema" << std::endl;
  os << "    Show the schemas of parquet files." << std::endl;
  os << "-p <path>" << std::endl;
  os << "  Parquet file path or a directory path that contains parquet "
        "files."
     << std::endl;
  os << "rowcount options:" << std::endl;
  os << "  -l Read rows w/o converting." << std::endl;
  os << "  -v Read rows converting to arrays of std::variant." << std::endl;
  os << "  -j Read rows converting to arrays of JSON objects." << std::endl;
}

void count_rows(const options_t& opt, ygm::comm& world) {
  if (opt.variant) {
    world.cout0() << "Read as variants." << std::endl;
  } else if (opt.json) {
    world.cout0() << "Read as JSON objects." << std::endl;
  } else if (opt.read_lines) {
    world.cout0() << "Read rows w/o converting." << std::endl;
  }

  ygm::io::arrow_parquet_parser parquetp(world, {opt.input_path.c_str()});
  const auto&                   schema = parquetp.schema();

  std::size_t num_rows        = 0;
  std::size_t num_error_lines = 0;

  ygm::timer timer{};
  if (opt.read_lines) {
    parquetp.for_all([&schema, &opt, &num_rows, &num_error_lines](
                         auto& stream_reader, const auto&) {
      if (opt.variant) {
        try {
          ygm::io::detail::read_parquet_as_variant(stream_reader, schema);
        } catch (...) {
          ++num_error_lines;
        }
      } else if (opt.json) {
        try {
          ygm::io::detail::read_parquet_as_json(stream_reader, schema);
        } catch (...) {
          ++num_error_lines;
        }
      } else {
        stream_reader.SkipColumns(schema.size());
        stream_reader.EndRow();
      }
      ++num_rows;
    });
    num_rows = world.all_reduce_sum(num_rows);
  } else {
    num_rows = parquetp.row_count();
  }
  const auto elapsed_time = timer.elapsed();

  world.cout0() << "Elapsed time: " << elapsed_time << " seconds" << std::endl;
  world.cout0() << "#of rows = " << num_rows << std::endl;
  if (opt.variant || opt.json) {
    world.cout0() << "#of conversion error lines = "
                  << world.all_reduce_sum(num_error_lines) << std::endl;
  }
}