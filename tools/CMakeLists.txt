# Copyright 2019-2024 Lawrence Livermore National Security, LLC and other YGM
# Project Developers. See the top-level COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

if (Arrow_FOUND AND Parquet_FOUND)
    if (Boost_FOUND)
        add_ygm_example(parquet_tools)
        target_include_directories(parquet_tools PUBLIC ${Boost_INCLUDE_DIRS})
        link_arrow_parquet(parquet_tools)
    endif ()
endif ()
file(COPY ./parquet_tools_subcmd.json DESTINATION ${CMAKE_CURRENT_BINARY_DIR})