#
# Sphinx
#
find_package(Sphinx)
if (Sphinx_FOUND)
    set(SPHINX_SOURCE ${CMAKE_CURRENT_SOURCE_DIR})
    set(SPHINX_BUILD ${CMAKE_CURRENT_BINARY_DIR}/sphinx)
    set(SPHINX_INDEX_FILE ${SPHINX_BUILD}/index.html)

    # Only regenerate Sphinx when: - Our doc files have been updated - Doxygen
    # has rerun - The Sphinx config has been updated
    add_custom_command(
        OUTPUT ${SPHINX_INDEX_FILE}
        COMMAND
            ${SPHINX_EXECUTABLE} -b html
            # Tell Breathe where to find the Doxygen output
            -Dbreathe_projects.ygm=${DOXYGEN_XML_OUTPUT} ${SPHINX_SOURCE}
            ${SPHINX_BUILD}
        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
        DEPENDS # Other docs files you want to track should go here (or in some
                # variable)
                ${CMAKE_CURRENT_SOURCE_DIR}/index.rst
                ${DOXYGEN_XML_OUTPUT}/index.xml
        MAIN_DEPENDENCY ${SPHINX_SOURCE}/conf.py
        COMMENT "Generating documentation with Sphinx"
    )

    # Nice named target so we can run the job easily
    add_custom_target(sphinx DEPENDS ${SPHINX_INDEX_FILE})

    # Add an install target to install the docs
    include(GNUInstallDirs)
    install(DIRECTORY ${SPHINX_BUILD} DESTINATION ${CMAKE_INSTALL_DOCDIR})
else ()
    message(STATUS "Sphinx not found")
endif ()
