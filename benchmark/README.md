# Benchmark

## Usage

### Setup

```bash
sudo apt install cppzmq-dev

mkdir cmake-build-debug
cd cmake-build-debug
cmake -S .. -B . -DCMAKE_BUILD_TYPE=Debug
cd ..

mkdir cmake-build-release
cd cmake-build-release
cmake -S .. -B . -DCMAKE_BUILD_TYPE=Release
cmake ..
cd ..
```

### Release Build

```bash
cmake --build ./cmake-build-release --target main -DCMAKE_BUILD_TYPE=Release && ./cmake-build-release/main
```

### Debug Build

```bash
cmake --build cmake-build-debug --target rocksdb -- -j$(nproc)
cmake --build ./cmake-build-debug --target main  && ./cmake-build-debug/main
```
