
cmake_minimum_required(VERSION 2.8)

project(BroPluginMySQL)

include(BroPlugin)

find_package(MySQL REQUIRED)
include_directories(BEFORE ${MYSQL_INCLUDE_DIR})
link_directories(${MYSQL_LIB_DIR})

bro_plugin_begin(WingPig MySQL)
bro_plugin_cc(src/Plugin.cc)
bro_plugin_cc(src/MySQLWriter.cc)
bro_plugin_bif(src/mysql.bif)
bro_plugin_dist_files(README CHANGES COPYING VERSION)
bro_plugin_link_library(${MYSQL_CLIENT_LIBS})
bro_plugin_end()

file(STRINGS "${CMAKE_CURRENT_SOURCE_DIR}/VERSION" VERSION LIMIT_COUNT 1)

if ("${PROJECT_SOURCE_DIR}" STREQUAL "${CMAKE_SOURCE_DIR}")
    # Allows building rpm/deb packages via "make package" in build dir.
    include(ConfigurePackaging)
    ConfigurePackaging(${VERSION})
endif ()
