# Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
# Project Developers. See the top-level COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

cmake_minimum_required(VERSION 3.14)

project(ygm
    VERSION 0.2
    DESCRIPTION "HPC Communication Library"
    LANGUAGES CXX)

if(CMAKE_PROJECT_NAME STREQUAL PROJECT_NAME)
    set(YGM_MAIN_PROJECT ON)
endif()

# Only do these if this is the main project, and not if it is included through add_subdirectory
if(YGM_MAIN_PROJECT)

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

#
# Create the YGM target library
#
add_library(ygm INTERFACE)
add_library(ygm::ygm ALIAS ygm)
target_include_directories(ygm INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)
target_compile_features(ygm INTERFACE cxx_std_17)

#
# MPI and Threads are required
#
find_package(MPI REQUIRED)
find_package(Threads REQUIRED)
target_link_libraries(ygm INTERFACE MPI::MPI_CXX Threads::Threads)

# prepare to FetchContent
include(FetchContent)

#
# Cereal
#
set(ygm_cereal_VERSION 1.3.0)
find_package(cereal QUIET)
if (NOT cereal_FOUND)
    # Currently cereal version 1.3.0 has an outdate CMakeLists.txt, so we need to use this commit
    FetchContent_Declare(
        cereal
        GIT_REPOSITORY https://github.com/USCiLab/cereal
        GIT_TAG        af0700efb25e7dc7af637b9e6f970dbb94813bff
    )
    set(JUST_INSTALL_CEREAL ON)
    FetchContent_MakeAvailable(cereal)
    message(STATUS "YGM cloned cereal dependency: " ${cereal_SOURCE_DIR})
else()
    message(STATUS "YGM found cereal dependency: " ${cereal_DIR})
endif()
target_link_libraries(ygm INTERFACE cereal)

#
# Boost
#
# only required by ygm/detail/cereal_boost_json.hpp. YGM will build if Boost is not found.
# The user must manually link to executable.
#
#find_package(Boost 1.75 COMPONENTS json)  # this would be better, but is not working on LC
find_package(Boost 1.75)
if (Boost_FOUND)
    message(STATUS "YGM found boost include dirs: " ${Boost_INCLUDE_DIRS})
else()
    message(WARNING "YGM did not find Boost >=1.75. Building without Boost.")
endif()

option(TEST_WITH_SLURM "Run tests with Slurm" OFF)


# Install ygm. Expects CMAKE_INSTALL_PREFIX to be set to a suitable directory.
option(YGM_INSTALL "Generate the install target" ${YGM_MAIN_PROJECT})
if(${YGM_INSTALL})
    include(GNUInstallDirs)
    include(CMakePackageConfigHelpers)

    # List libraries to be installed
    set(YGM_EXPORT_TARGETS ygm)
    if(NOT cereal_FOUND)
        # If cereal is not installed in the system, include the cloned headers as a dependency.
	set(YGM_EXPORT_TARGETS ${YGM_EXPORT_TARGETS} cereal)
    endif()

    install(TARGETS ${PROJECT_NAME}
            EXPORT ${PROJECT_NAME}Targets
            ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
            LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
            RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})

    install(EXPORT ${PROJECT_NAME}Targets
            FILE ${PROJECT_NAME}Targets.cmake
            NAMESPACE ${PROJECT_NAME}::
            DESTINATION ${CMAKE_INSTALL_DATAROOTDIR}/${PROJECT_NAME}/cmake)

    install(DIRECTORY ${PROJECT_SOURCE_DIR}/include/ygm
            DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})


    if(NOT cereal_FOUND)
        install(TARGETS cereal
                EXPORT cerealTargets
                ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
                LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
                RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})

        install(EXPORT cerealTargets
                FILE cerealTargets.cmake
                DESTINATION ${CMAKE_INSTALL_DATAROOTDIR}/cereal/cmake)

        install(DIRECTORY ${PROJECT_BINARY_DIR}/_deps/cereal-src/include/cereal
	            DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})

        write_basic_package_version_file(
            "cerealConfigVersion.cmake"
	        VERSION ${ygm_cereal_VERSION}
	        COMPATIBILITY ExactVersion)

        configure_package_config_file(
	        "${PROJECT_SOURCE_DIR}/cmake/cerealConfig.cmake.in"
	        "${PROJECT_BINARY_DIR}/cerealConfig.cmake"
	        INSTALL_DESTINATION ${CMAKE_INSTALL_DATAROOTDIR}/cereal/cmake)

        install(FILES
	        "${PROJECT_BINARY_DIR}/cerealConfig.cmake"
	        "${PROJECT_BINARY_DIR}/cerealConfigVersion.cmake"
	        DESTINATION ${CMAKE_INSTALL_DATAROOTDIR}/cereal/cmake)
    endif()

    # create version file
    write_basic_package_version_file(
        "${PROJECT_NAME}ConfigVersion.cmake"
  	    VERSION ${PROJECT_VERSION}
	      COMPATIBILITY ExactVersion)

    # create config file
    configure_package_config_file(
        "${PROJECT_SOURCE_DIR}/cmake/${PROJECT_NAME}Config.cmake.in"
	    "${PROJECT_BINARY_DIR}/${PROJECT_NAME}Config.cmake"
	    INSTALL_DESTINATION ${CMAKE_INSTALL_DATAROOTDIR}/${PROJECT_NAME}/cmake)

    install(FILES
        "${PROJECT_BINARY_DIR}/${PROJECT_NAME}Config.cmake"
  	    "${PROJECT_BINARY_DIR}/${PROJECT_NAME}ConfigVersion.cmake"
	    DESTINATION ${CMAKE_INSTALL_DATAROOTDIR}/${PROJECT_NAME}/cmake)


endif()


if(JUST_INSTALL_YGM)
    return()
endif()

# Testing & examples are only available if this is the main app
if(YGM_MAIN_PROJECT)
    add_subdirectory(test)
    add_subdirectory(performance)
    # Example codes are here.
    add_subdirectory(examples)
endif()
