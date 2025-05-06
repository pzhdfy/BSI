 cmake -G Ninja    -DCMAKE_C_COMPILER=$(brew --prefix llvm@16)/bin/clang -DCMAKE_CXX_COMPILER=$(brew --prefix llvm@16)/bin/clang++  -DCMAKE_EXPORT_COMPILE_COMMANDS=ON -S . -B build
