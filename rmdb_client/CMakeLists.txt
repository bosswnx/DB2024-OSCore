set(CMAKE_CXX_STANDARD 20)
find_package(Threads REQUIRED)

add_executable(rmdb_client main.cpp)

target_link_libraries(rmdb_client
        pthread readline 
)