include(PythonUtilities)

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
function(find_pip_installed_pyarrow)
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
        endif ()
    else ()
        message(FATAL_ERROR "PIP_PYARROW_ROOT is not set. PIP_PYARROW_ROOT must be set to the root of the pyarrow installation.")
    endif ()

endfunction()


# Find the directory where pyarrow is installed.
# This function executes a Python script to find the pyarrow module and
# **does not assume that pyarrow is installed by pip**.
#
# Output:
# PYARROW_ROOT is set to the root of the pyarrow installation.
function(find_pyarrow_package)
    find_python3_module(pyarrow)
    if (PYTHON3_MODULE_PATH)
        get_filename_component(PYARROW_ROOT ${PYTHON3_MODULE_PATH} DIRECTORY)
        set(PYARROW_ROOT ${PYARROW_ROOT} PARENT_SCOPE)
    endif ()
endfunction()

# Install pyarrow using pip
# Output:
# PIP_PYARROW_ROOT is set to the root of the pyarrow installation.
function(install_pyarrow_in_venv)
    setup_python_venv()
    if (NOT PYTHON_VENV_ROOT)
        return()
    endif ()

    activate_python_venv(${PYTHON_VENV_ROOT})
    if (NOT PYTHON_VENV_ACTIVATED)
        return()
    endif ()

    # Use only the Python 3 interpreter in the virtual environment
    set(Python3_FIND_VIRTUALENV ONLY)

    # Upgrade pip
    # Ignore the error status as failing to upgrade is not the end of the world
    upgrade_pip()

    # Install pyarrow
    pip_install_python_package("pyarrow==16.1.*")
    if (PIP_INSTALL_SUCCEEDED)
        find_pyarrow_package()
        if (PYARROW_ROOT)
            set(PIP_PYARROW_ROOT ${PYARROW_ROOT} PARENT_SCOPE)
        endif ()
    endif ()

    deactivate_python_venv()
endfunction()


# Find Arrow and Parquet using find_package
# Output:
# Arrow_FOUND is set to TRUE if Arrow is found.
# Parquet_FOUND is set to TRUE if Parquet is found.
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
    endif ()
endfunction()

# Find Arrow and Parquet. If not found, install pyarrow using pip in a Python virtual environmental space.
# Input:
#   YGM_REQUIRE_PARQUET (option)
#       If TRUE, a fatal error is thrown when Arrow Parquet is not found or not installed.
#   YGM_INSTALL_PARQUET (option)
#       If TRUE, install Arrow Parquet when it is not found.
#   PIP_PYARROW_ROOT (option)
#       The root directory of a pyarrow installed by pip. If given, search only the location.
#       If Arrow Parquet is not found, A) return this function or B) throw a fatal error if YGM_REQUIRE_PARQUET is true.
#       Ignore YGM_INSTALL_PARQUET --- no installation is performed even if Arrow Parquet is not found.
# Output:
#   Arrow_FOUND and Parquet_FOUND are defined and set to TRUE if Arrow and Parquet are found.
function(find_or_install_arrow_parquet)
    if (PIP_PYARROW_ROOT)
        find_pip_installed_pyarrow()
        if (NOT Arrow_FOUND OR NOT Parquet_FOUND)
            if (YGM_REQUIRE_PARQUET)
                message(FATAL_ERROR "${PROJECT_NAME} requires Arrow Parquet but Arrow Parquet was not found in ${PIP_PYARROW_ROOT}.")
            else ()
                message(WARNING "${PROJECT_NAME} did not find Arrow Parquet in ${PIP_PYARROW_ROOT}. Building without Arrow Parquet.")
            endif ()
            # If Arrow Parquet is not found, exit this function
            return()
        endif ()
    endif ()

    if (NOT Arrow_FOUND OR NOT Parquet_FOUND)
        find_arrow_parquet_config()
    endif ()

    if (NOT Arrow_FOUND OR NOT Parquet_FOUND)
        find_pyarrow_package()
        if (PYARROW_ROOT)
            # Assume that the found pip was installed by pip.
            set(PIP_PYARROW_ROOT ${PYARROW_ROOT})
            find_pip_installed_pyarrow()
        endif ()
    endif ()

    if (NOT Arrow_FOUND OR NOT Parquet_FOUND)
        if (YGM_INSTALL_PARQUET)
            install_pyarrow_in_venv()
            if (PIP_PYARROW_ROOT)
                find_pip_installed_pyarrow()
            endif ()
        endif ()
    endif ()

    if (NOT Arrow_FOUND OR NOT Parquet_FOUND)
        message(STATUS "${PROJECT_NAME} could not find or install Arrow Parquet.")
        message(STATUS "If this is an unexpected result, try the following command to install pyarrow: export Python3_ROOT_DIR=/path/to/python3; /path/to/python3 -m pip pyarrow")
        if (YGM_REQUIRE_PARQUET)
            message(FATAL_ERROR "${PROJECT_NAME} requires Arrow Parquet.")
        else ()
            message(WARNING "${PROJECT_NAME} keep the build process without Arrow Parquet.")
        endif ()
        return()
    endif ()

    set(Arrow_FOUND TRUE PARENT_SCOPE)
    set(Parquet_FOUND TRUE PARENT_SCOPE)
endfunction()


# Link Arrow and Parquet to the target
# This function must be called after find_or_install_arrow_parquet().
function(link_arrow_parquet target)
    if (Arrow_FOUND AND Parquet_FOUND)
        target_link_libraries(${target} PUBLIC
                Arrow::arrow_shared Parquet::parquet_shared)
    else ()
        message(WARNING "Arrow or Parquet not found. Not linking Arrow or Parquet.")
    endif ()
endfunction()
