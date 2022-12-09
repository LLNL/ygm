# Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
# Project Developers. See the top-level COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

if (Arrow_FOUND AND Parquet_FOUND)
    add_ygm_example(arrow_parquet_stream_reader)
    target_link_libraries(arrow_parquet_stream_reader PUBLIC arrow_shared parquet_shared)
endif()

