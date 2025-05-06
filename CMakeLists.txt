cmake_minimum_required(VERSION 3.10)
project(RoaringExample)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)  # 这一句很关键！

# 添加源码
add_executable(main
    main.cpp
    roaring.c
)

# 包含当前目录的头文件
target_include_directories(main PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

find_package(fmt REQUIRED)
target_link_libraries(main PRIVATE fmt::fmt)
