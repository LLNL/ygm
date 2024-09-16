# Find Arrow and Parquet using find_package
function(find_arrow_parquet_config)
    # Find Arrow >= 8.0.
    # Start major version from 100 so that we do not have to update
    # this code every time Arrow releases a major version.
    foreach (MAJOR_VERSION RANGE 100 8 -1)
        find_package(Arrow "${MAJOR_VERSION}.0" QUIET)
        if (Arrow_FOUND)
            break()
        endif ()
    endforeach ()
    set(Arrow_FOUND ${Arrow_FOUND} PARENT_SCOPE)

    # Find Parquet
    if (Arrow_FOUND)
        find_package(Parquet QUIET PATHS ${Arrow_DIR})
    endif ()
    set(Parquet_FOUND ${Parquet_FOUND} PARENT_SCOPE)

    # Show Arrow and Parquet info
    if (Arrow_FOUND AND Parquet_FOUND)
        if (Arrow_FOUND)
            message(STATUS ${PROJECT_NAME} " found Arrow")
            message(STATUS "Arrow version: ${ARROW_VERSION}")
            message(STATUS "Arrow SO version: ${ARROW_FULL_SO_VERSION}")
        endif ()

        if (Parquet_FOUND)
            message(STATUS ${PROJECT_NAME} " found Parquet")
            message(STATUS "Parquet version: ${PARQUET_VERSION}")
            message(STATUS "Parquet SO version: ${PARQUET_FULL_SO_VERSION}")
        endif ()
    else ()
        if (YGM_REQUIRE_ARROW_PARQUET)
            message(FATAL_ERROR "${PROJECT_NAME} requires Arrow Parquet >= 8.0 but Arrow Parquet was not found.")
        else ()
            message(WARNING "${PROJECT_NAME} did not find Arrow Parquet >= 8.0. Building without Arrow Parquet.")
        endif ()
    endif ()
endfunction()


# Find Arrow and Parquet installed along with pyarrow by pip.
#
# Input:
# PIP_PYARROW_ROOT must be set to the root of the pyarrow installation.
# This function assumes the following directory structure:
# PIP_PYARROW_ROOT
# ├── include
# │   ├── arrow
# │   │   ├── ...
# │   ├── parquet
# │   │   ├── ...
# ├── libarrow.so.XXXX
# ├── libparquet.so.XXXX
#
# XXXX is the version number.
#
# Output:
# If Arrow and Parquet are found, set Arrow_FOUND and Parquet_FOUND to TRUE.
# Also, Arrow::arrow_shared and Parquet::parquet_shared are created as imported targets.
# Those targets can be used to link Arrow and Parquet as find_package() is used.
function(find_pyarrow)
    if (PIP_PYARROW_ROOT)
        # Find libarrow
        file(GLOB Arrow_LIBRARIES LIST_DIRECTORIES false "${PIP_PYARROW_ROOT}/libarrow.so.*")
        if (Arrow_LIBRARIES)
            set(Arrow_FOUND TRUE)
        else ()
            set(Arrow_FOUND FALSE)
        endif ()

        # Find libparquet
        file(GLOB Parquet_LIBRARIES LIST_DIRECTORIES false "${PIP_PYARROW_ROOT}/libparquet.so.*")
        if (Parquet_LIBRARIES)
            set(Parquet_FOUND TRUE)
        else ()
            set(Parquet_FOUND FALSE)
        endif ()

        # NO_DEFAULT_PATH is to avoid finding the system-installed Parquet
        find_path(Arrow_INCLUDE_DIRS NAMES parquet/types.h PATHS "${PIP_PYARROW_ROOT}/include" NO_DEFAULT_PATH)
        if (NOT Arrow_INCLUDE_DIRS)
            set(Arrow_FOUND FALSE)
            set(Parquet_FOUND FALSE)
        endif ()

        # Propagate the results to the parent scope
        set(Arrow_FOUND ${Arrow_FOUND} PARENT_SCOPE)
        set(Parquet_FOUND ${Parquet_FOUND} PARENT_SCOPE)

        # Create imported targets
        if (Arrow_FOUND)
            add_library(Arrow::arrow_shared SHARED IMPORTED GLOBAL)
            set_target_properties(Arrow::arrow_shared PROPERTIES
                    IMPORTED_LOCATION ${Arrow_LIBRARIES}
                    INTERFACE_INCLUDE_DIRECTORIES ${Arrow_INCLUDE_DIRS})
        endif ()
        if (Parquet_FOUND)
            add_library(Parquet::parquet_shared SHARED IMPORTED GLOBAL)
            set_target_properties(Parquet::parquet_shared PROPERTIES
                    IMPORTED_LOCATION ${Parquet_LIBRARIES}
                    INTERFACE_INCLUDE_DIRECTORIES ${Arrow_INCLUDE_DIRS})
        endif ()

        # Show Arrow and Parquet info
        if (Arrow_FOUND AND Parquet_FOUND)
            if (Arrow_FOUND)
                message(STATUS ${PROJECT_NAME} " found libarrow")
                message(STATUS "libarrow: ${Arrow_LIBRARIES}")
            endif ()

            if (Parquet_FOUND)
                message(STATUS ${PROJECT_NAME} " found libparquet")
                message(STATUS "libparquet: ${Parquet_LIBRARIES}")
            endif ()

            message(STATUS "Arrow include dir: ${Arrow_INCLUDE_DIRS}")
        else () # Arrow or Parquet not found
            if (YGM_REQUIRE_ARROW_PARQUET)
                message(FATAL_ERROR "${PROJECT_NAME} requires Arrow Parquet but Arrow Parquet was not found.")
            else ()
                message(WARNING "${PROJECT_NAME} did not find Arrow Parquet. Building without Arrow Parquet.")
            endif ()
        endif ()
    else ()
        message(FATAL_ERROR "PIP_PYARROW_ROOT is not set. PIP_PYARROW_ROOT must be set to the root of the pyarrow installation.")
    endif ()

endfunction()


# Find Arrow and Parquet using find_arrow or find_pyarrow
# If PIP_PYARROW_ROOT is set, find_pyarrow is used.
#
# Output:
# Arrow_FOUND and Parquet_FOUND are set to TRUE if Arrow and Parquet are found.
function(find_arrow_parquet)
    if (PIP_PYARROW_ROOT)
        find_pyarrow()
    else ()
        find_arrow_parquet_config()
    endif ()
    set(Arrow_FOUND ${Arrow_FOUND} PARENT_SCOPE)
    set(Parquet_FOUND ${Parquet_FOUND} PARENT_SCOPE)
endfunction()


# Link Arrow and Parquet to the target
# This function must be called after find_arrow_parquet().
function(link_arrow_parquet target)
    if (Arrow_FOUND AND Parquet_FOUND)
        target_link_libraries(${target} PUBLIC
                Arrow::arrow_shared Parquet::parquet_shared)
    else ()
        message(WARNING "Arrow or Parquet not found. Not linking Arrow or Parquet.")
    endif ()
endfunction()