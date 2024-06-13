# Copyright 2019-2023 Lawrence Livermore National Security, LLC and other YGM
# Project Developers. See the top-level COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

if (Arrow_FOUND AND Parquet_FOUND)
    add_ygm_example(parquet_stream_reader)
    link_arrow_parquet(parquet_stream_reader)

    add_ygm_example(parquet_stream_reader_variant)
    link_arrow_parquet(parquet_stream_reader_variant)

    if (Boost_FOUND)
        add_ygm_example(parquet_stream_reader_json)
        target_include_directories(parquet_stream_reader_json PUBLIC ${Boost_INCLUDE_DIRS})
        link_arrow_parquet(parquet_stream_reader_json)
    endif ()
else ()
    message(STATUS "Arrow or Parquet not found, skipping bulding arrow_parquet examples.")
endif ()