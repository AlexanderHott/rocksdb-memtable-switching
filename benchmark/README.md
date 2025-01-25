## Usage

### Setup

```bash
sudo apt install cppzmq-dev

mkdir cmake-build-debug
cd cmake-build-debug
cmake ..
cd ..

mkdir cmake-build-release
cd cmake-build-release
cmake ..
cd ..
```

### Release Build
```bash
cmake --build ./cmake-build-release --target main -DCMAKE_BUILD_TYPE=Release && ./cmake-build-debug/main
```

### Debug Build
```bash
cmake --build cmake-build-debug --target rocksdb -- -j32
cmake --build ./cmake-build-debug --target main  && ./cmake-build-debug/main
```
