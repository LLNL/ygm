# Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
# Project Developers. See the top-level COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

# Works with 3.11 and tested through 3.15 (not tested yet)
cmake_minimum_required(VERSION 3.11...3.15)

project(YGM
    VERSION 0.2
    DESCRIPTION "HPC Communication Library"
    LANGUAGES CXX)

if(CMAKE_PROJECT_NAME STREQUAL PROJECT_NAME)
    set(YGM_MAIN_PROJECT ON)
endif()

# Only do these if this is the main project, and not if it is included through add_subdirectory
if(CMAKE_MAIN_PROJECT)

    # Let's nicely support folders in IDE's
    set_property(GLOBAL PROPERTY USE_FOLDERS ON)

    # Testing only available if this is the main app
    # Note this needs to be done in the main CMakeLists
    # since it calls enable_testing, which must be in the
    # main CMakeLists.
    include(CTest)

    # Docs only available if this is the main app
    find_package(Doxygen)
    if(Doxygen_FOUND)
	#add_subdirectory(docs)
    else()
	message(STATUS "Doxygen not found, not building docs")
    endif()
endif()

### Require out-of-source builds
file(TO_CMAKE_PATH "${PROJECT_BINARY_DIR}/CMakeLists.txt" LOC_PATH)
if(EXISTS "${LOC_PATH}")
    message(FATAL_ERROR "You cannot build in a source directory (or any directory with a CMakeLists.txt file). Please make a build subdirectory. Feel free to remove CMakeCache.txt and CMakeFiles.")
endif()

add_library(ygm INTERFACE)
add_library(YGM::ygm ALIAS ygm)
target_include_directories(ygm INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)
target_compile_features(ygm INTERFACE cxx_std_17)

#
# MPI is required
#
find_package(MPI REQUIRED)

#
# Boost
#
find_package(Boost 1.75 REQUIRED)

#
# Cereal
#
include(FetchContent)
set(JUST_INSTALL_CEREAL ON)
FetchContent_Declare(
        cereal
        GIT_REPOSITORY https://github.com/USCiLab/cereal
        GIT_TAG        af0700efb25e7dc7af637b9e6f970dbb94813bff
)
FetchContent_MakeAvailable(cereal)

option(TEST_WITH_SLURM "Run tests with Slurm" OFF)

option(YGM_INSTALL "Generate the install target" ${YGM_MAIN_PROJECT})
if(YGM_INSTALL)
    include(CMakePackageConfigHelpers)

    install(TARGETS cereal EXPORT ${PROJECT_NAME}Targets)
    install(DIRECTORY include/ygm DESTINATION include)

    set(configFile ${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}Config.cmake)
    set(versionFile ${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}ConfigVersion.cmake)
    set(configInstallDestination lib/cmake/${PROJECT_NAME})

    configure_package_config_file(
        ${CMAKE_CURRENT_SOURCE_DIR}/Config.cmake.in
	${configFile}
	INSTALL_DESTINATION ${configInstallDestination}
    )

    install(FILES ${configFile} ${versionFile} DESTINATION ${configInstallDestination})
    install(
	EXPORT ${PROJECT_NAME}Targets
	NAMESPACE "ygm::"
	DESTINATION ${configInstallDestination}
    )
endif()




# Header-only library, so likely not have src dir 
# add_subdirectory(src)

# Testing & examples are only available if this is the main app
# Emergency override MODERN_CMAKE_BUILD_TESTING provided as well
#if((CMAKE_PROJECT_NAME STREQUAL PROJECT_NAME OR MODERN_CMAKE_BUILD_TESTING) AND BUILD_TESTING)
if(YGM_MAIN_PROJECT)
    add_subdirectory(test)
    add_subdirectory(performance)
    # Example codes are here.
    add_subdirectory(examples)
endif()
