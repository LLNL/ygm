#
# Doxygen
#

find_package(Doxygen)
if (NOT DOXYGEN_FOUND)
    message(
        WARNING "Doxygen not found, will skip Doxygen documentation generation."
    )
    return()
endif ()

# Set up the Doxygen configuration file
set(DOXYGEN_GENERATE_HTML YES)
set(DOXYGEN_HTML_OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/html)
set(DOXYGEN_GENERATE_XML YES)
set(DOXYGEN_XML_OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/xml)
set(DOXYGEN_EXAMPLE_PATH ${PROJECT_SOURCE_DIR}/examples)
set(DOXYGEN_EXAMPLE_RECURSIVE YES)
set(DOXYGEN_SOURCE_BROWSER YES)
set(DOXYGEN_ENABLE_PREPROCESSING YES)
set(DOXYGEN_EXTRACT_ALL YES)
set(DOXYGEN_EXTRACT_STATIC YES)

# Add a target 'doxygen' Run 'make doxygen' to generate the docs
doxygen_add_docs(
    doxygen ${PROJECT_SOURCE_DIR}/include
    COMMENT "Generate documentation by Doxygen"
)

add_subdirectory(rtd)
