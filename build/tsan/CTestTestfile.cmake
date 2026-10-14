# CMake generated Testfile for 
# Source directory: /home/mayank/Desktop/CN
# Build directory: /home/mayank/Desktop/CN/build/tsan
# 
# This file includes the relevant testing commands required for 
# testing this directory and lists subdirectories to be tested as well.
add_test([=[unit_tests]=] "/home/mayank/Desktop/CN/build/tsan/unit_tests")
set_tests_properties([=[unit_tests]=] PROPERTIES  _BACKTRACE_TRIPLES "/home/mayank/Desktop/CN/CMakeLists.txt;102;add_test;/home/mayank/Desktop/CN/CMakeLists.txt;0;")
add_test([=[simulation]=] "/home/mayank/Desktop/CN/build/tsan/simulation")
set_tests_properties([=[simulation]=] PROPERTIES  _BACKTRACE_TRIPLES "/home/mayank/Desktop/CN/CMakeLists.txt;103;add_test;/home/mayank/Desktop/CN/CMakeLists.txt;0;")
subdirs("_deps/doctest-build")
