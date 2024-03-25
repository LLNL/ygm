# Copyright 2019-2023 Lawrence Livermore National Security, LLC and other YGM
# Project Developers. See the top-level COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

if (Arrow_FOUND AND Parquet_FOUND)
    add_ygm_example(arrow_parquet_stream_reader)
    target_link_libraries(arrow_parquet_stream_reader PUBLIC
        Arrow::arrow_shared Parquet::parquet_shared)

    add_ygm_example(arrow_parquet_stream_reader_variant)
    target_link_libraries(arrow_parquet_stream_reader_variant PUBLIC
            Arrow::arrow_shared Parquet::parquet_shared)

    if (Boost_FOUND)
        add_ygm_example(arrow_parquet_stream_reader_json)
        target_include_directories(arrow_parquet_stream_reader_json PUBLIC ${Boost_INCLUDE_DIRS})
        target_link_libraries(arrow_parquet_stream_reader_json PUBLIC
            Arrow::arrow_shared Parquet::parquet_shared)
    endif()
endif()