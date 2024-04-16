# Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
# Project Developers. See the top-level COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

cmake_minimum_required(VERSION 3.14)

project(
    ygm
    VERSION 0.6
    DESCRIPTION "HPC Communication Library"
    LANGUAGES CXX
)

if (CMAKE_PROJECT_NAME STREQUAL PROJECT_NAME)
    set(YGM_MAIN_PROJECT ON)
endif ()

# Controls whether to set up install boilerplate for project and depencencies.
# Expects CMAKE_INSTALL_PREFIX to be set to a suitable directory.
option(YGM_INSTALL "Generate the install target" ${YGM_MAIN_PROJECT})

# Only do these if this is the main project, and not if it is included through
# add_subdirectory
if (YGM_MAIN_PROJECT)

    # Let's nicely support folders in IDE's
    set_property(GLOBAL PROPERTY USE_FOLDERS ON)

    # Testing only available if this is the main app Note this needs to be done
    # in the main CMakeLists since it calls enable_testing, which must be in the
    # main CMakeLists.
    include(CTest)

    # Docs only available if this is the main app
    find_package(Doxygen)
    if (Doxygen_FOUND)
        # add_subdirectory(docs)
    else ()
        message(STATUS "Doxygen not found, not building docs")
    endif ()
endif ()

# Require out-of-source builds
file(TO_CMAKE_PATH "${PROJECT_BINARY_DIR}/CMakeLists.txt" LOC_PATH)
if (EXISTS "${LOC_PATH}")
    message(
        FATAL_ERROR
            "You cannot build in a source directory (or any directory with a CMakeLists.txt file). Please make a build subdirectory. Feel free to remove CMakeCache.txt and CMakeFiles."
    )
endif ()

#
# MPI and Threads are required
#
find_package(MPI REQUIRED)
find_package(Threads REQUIRED)

# prepare to FetchContent
include(FetchContent)

#
# Cereal
#
set(YGM_CEREAL_VERSION 1.3.0)
# Would like to version this find, but the spack config file is versionless so
# will always throw an error.
set(YGM_CEREAL_TARGET "cereal::cereal")
find_package(cereal CONFIG QUIET)
if (NOT cereal_FOUND)
    # Currently cereal version 1.3.0 has an outdated CMakeLists.txt, so we need
    # to use this specific commit for now.
    FetchContent_Declare(
        cereal
        GIT_REPOSITORY https://github.com/USCiLab/cereal.git
        GIT_TAG af0700efb25e7dc7af637b9e6f970dbb94813bff
    )
    FetchContent_GetProperties(cereal)
    if (cereal_POPULATED)
        message(STATUS ${PROJECT_NAME}
                       " found already populated cereal dependency: "
                       ${cereal_SOURCE_DIR}
        )
    else ()
        # Do not compile any cereal tests
        set(JUST_INSTALL_CEREAL ON)
        # Install cereal at ${CMAKE_INSTALL_PREFIX}
        set(CEREAL_INSTALL ${YGM_INSTALL})
        # Populate cereal
        FetchContent_Populate(cereal)
        # Include cereal root cmake boilerplate
        add_subdirectory(${cereal_SOURCE_DIR} ${cereal_BINARY_DIR})

        message(STATUS ${PROJECT_NAME} " cloned cereal dependency: "
                       ${cereal_SOURCE_DIR}
        )
    endif ()
else ()
    message(STATUS ${PROJECT_NAME} " found installed cereal dependency: "
                   ${cereal_DIR}
    )
    # cereal installed with spack creates library target "cereal", whereas
    # installing from source creates target "cereal::cereal". This is the only
    # simple way I could figure out how to differentiate the two, but this will
    # cause problems if a spack instance installs headers to a path that does
    # not include the substring "spack".
    if (${cereal_DIR} MATCHES ".*spack.*")
        set(YGM_CEREAL_TARGET "cereal")
    else ()
        # cereal::cereal should be the right target
    endif ()
endif ()

#
# Boost
#
# only required by ygm/detail/cereal_boost_json.hpp. YGM will build if Boost is
# not found. The user must manually link Boost to the executable.
#
# find_package(Boost 1.75 COMPONENTS json)  # this would be better, but is not
# working on LC
find_package(Boost 1.75)
if (Boost_FOUND)
    message(STATUS ${PROJECT_NAME} " found boost include dirs: "
                   ${Boost_INCLUDE_DIRS}
    )
else ()
    message(WARNING ${PROJECT_NAME}
                    " did not find Boost >=1.75. Building without Boost."
    )
endif ()

#
# Arrow
#
#
find_package(Arrow 8.0 QUIET)
if (NOT Arrow_FOUND)
	find_package(Arrow 9.0 QUIET)
endif()
if (NOT Arrow_FOUND)
	find_package(Arrow 10.0 QUIET)
endif()
if (NOT Arrow_FOUND)
	find_package(Arrow 11.0 QUIET)
endif()
if (NOT Arrow_FOUND)
	find_package(Arrow 12.0 QUIET)
endif()
if (NOT Arrow_FOUND)
	find_package(Arrow 13.0 QUIET)
endif()
if (NOT Arrow_FOUND)
	find_package(Arrow 14.0 QUIET)
endif()
if (NOT Arrow_FOUND)
	find_package(Arrow 15.0 QUIET)
endif()
if (Arrow_FOUND)
   message(STATUS ${PROJECT_NAME} " found Arrow ")
   message(STATUS "Arrow version: ${ARROW_VERSION}")
   message(STATUS "Arrow SO version: ${ARROW_FULL_SO_VERSION}")
   set(ARROW_CMAKE_DIR ${Arrow_DIR})
   find_package(Parquet PATHS ${ARROW_CMAKE_DIR})
   if (Parquet_FOUND)
       message(STATUS ${PROJECT_NAME} " found Parquet ")
       message(STATUS "Parquet version: ${PARQUET_VERSION}")
       message(STATUS "Parquet SO version: ${PARQUET_FULL_SO_VERSION}")
   else ()
       message(WARNING ${PROJECT_NAME} " did not find Parquet. Building without Parquet.")
   endif ()
else ()
   message(WARNING ${PROJECT_NAME} " did not find Arrow >= 8.0. Building without Arrow.")
   if (YGM_REQUIRE_ARROW)
	   message(FATAL_ERROR "YGM configured to require Arrow, but Arrow could not be found")
   endif ()
endif ()

#
# Create the YGM target library
#
add_library(ygm INTERFACE)
add_library(ygm::ygm ALIAS ygm)
target_compile_features(ygm INTERFACE cxx_std_17)
target_include_directories(
    ygm INTERFACE $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
                  $<INSTALL_INTERFACE:include>
)
target_link_libraries(
    ygm INTERFACE MPI::MPI_CXX Threads::Threads stdc++fs ${YGM_CEREAL_TARGET}
)

option(TEST_WITH_SLURM "Run tests with Slurm" OFF)

# Install ygm. Expects CMAKE_INSTALL_PREFIX to be set to a suitable directory.
if (${YGM_INSTALL})
    include(GNUInstallDirs)
    include(CMakePackageConfigHelpers)

    # List libraries to be installed
    set(YGM_EXPORT_TARGETS ygm)

    install(
        TARGETS ${PROJECT_NAME}
        EXPORT ${PROJECT_NAME}Targets
        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
        RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
    )

    install(
        EXPORT ${PROJECT_NAME}Targets
        FILE ${PROJECT_NAME}Targets.cmake
        NAMESPACE ${PROJECT_NAME}::
        DESTINATION ${CMAKE_INSTALL_DATAROOTDIR}/${PROJECT_NAME}/cmake
    )

    install(DIRECTORY ${PROJECT_SOURCE_DIR}/include/ygm
            DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
    )

    # create version file
    write_basic_package_version_file(
        "${PROJECT_NAME}ConfigVersion.cmake"
        VERSION ${PROJECT_VERSION}
        COMPATIBILITY ExactVersion
    )

    # create config file
    configure_package_config_file(
        "${PROJECT_SOURCE_DIR}/cmake/${PROJECT_NAME}Config.cmake.in"
        "${PROJECT_BINARY_DIR}/${PROJECT_NAME}Config.cmake"
        INSTALL_DESTINATION ${CMAKE_INSTALL_DATAROOTDIR}/${PROJECT_NAME}/cmake
    )

    install(FILES "${PROJECT_BINARY_DIR}/${PROJECT_NAME}Config.cmake"
                  "${PROJECT_BINARY_DIR}/${PROJECT_NAME}ConfigVersion.cmake"
            DESTINATION ${CMAKE_INSTALL_DATAROOTDIR}/${PROJECT_NAME}/cmake
    )

endif ()

option(JUST_INSTALL_YGM "Skip executable compilation" OFF)
if (JUST_INSTALL_YGM)
    return()
endif ()

if (NOT CMAKE_BUILD_TYPE)
	set(CMAKE_BUILD_TYPE Release)
	message(STATUS "CMAKE_BUILD_TYPE is set as Release")
endif ()

# Testing & examples are only available if this is the main app
if (YGM_MAIN_PROJECT)
    add_subdirectory(test)
    # Example codes are here.
    add_subdirectory(examples)
endif ()
