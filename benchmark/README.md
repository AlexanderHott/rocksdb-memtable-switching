## Usage

```bash
# run once
sudo apt install cppzmq-dev
mkdir cmake-build-debug
cd cmake-build-debug
cmake ..
cd ..

# build
cmake --build ./cmake-build-debug --target main
# run
./cmake-build-debug/main

# build and run
cmake --build ./cmake-build-debug --target main && ./cmake-build-debug/main
```
