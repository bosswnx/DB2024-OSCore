cd build
cmake .. -DCMAKE_BUILD_TYPE=Debug
make rmdb -j8
cd ../rmdb_client/build
cmake .. -DCMAKE_BUILD_TYPE=Debug
make rmdb_client -j8