# CMake generated Testfile for 
# Source directory: /home/oysyn/Date_Index/anti-plagiarism/Back_L5
# Build directory: /home/oysyn/Date_Index/anti-plagiarism/Back_L5/build_asan
# 
# This file includes the relevant testing commands required for 
# testing this directory and lists subdirectories to be tested as well.
add_test(test_build_smoke "/home/oysyn/Date_Index/anti-plagiarism/Back_L5/build_asan/test_build_smoke")
set_tests_properties(test_build_smoke PROPERTIES  _BACKTRACE_TRIPLES "/home/oysyn/Date_Index/anti-plagiarism/Back_L5/CMakeLists.txt;91;add_test;/home/oysyn/Date_Index/anti-plagiarism/Back_L5/CMakeLists.txt;0;")
add_test(test_validate "/home/oysyn/Date_Index/anti-plagiarism/Back_L5/build_asan/test_validate")
set_tests_properties(test_validate PROPERTIES  _BACKTRACE_TRIPLES "/home/oysyn/Date_Index/anti-plagiarism/Back_L5/CMakeLists.txt;96;add_test;/home/oysyn/Date_Index/anti-plagiarism/Back_L5/CMakeLists.txt;0;")
add_test(test_search_smoke "/home/oysyn/Date_Index/anti-plagiarism/Back_L5/build_asan/test_search_smoke")
set_tests_properties(test_search_smoke PROPERTIES  _BACKTRACE_TRIPLES "/home/oysyn/Date_Index/anti-plagiarism/Back_L5/CMakeLists.txt;101;add_test;/home/oysyn/Date_Index/anti-plagiarism/Back_L5/CMakeLists.txt;0;")
