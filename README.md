azure-kusto-avro-conv
=====================

Utility that converts Avro files to JSON format.


## Building in Linux

### Prerequisites

 * CMake >= 3.12.
 * libavro-dev

### Compiling

    mkdir build
    cd build
    cmake ..
    make

Sometimes, `zlib` can't be found by CMake, in such cases it's required to set the path to the library explicitly using:

    cmake -DZLIB_LIBRARY=/usr/lib/x86_64-linux-gnu/libz.a ..

## Building in Windows

### Prerequisites

 * Microsoft Visual Studio 2019.
 * CMake >= 3.12.
 * Private fork of [vcpkg](https://github.com/spektom/vcpkg).
 * Apache Avro installed with `vcpkg` (see below).

To install Apache Avro library, run:

    git clone https://github.com/spektom/vcpkg.git
    cd vcpkg
    git checkout avro_c_snappy
    .\bootstrap-vcpkg.bat
    .\vcpkg install avro-c:x64-windows

### Compiling

    mkdir build
    cd build
    cmake -DCMAKE_TOOLCHAIN_FILE=C:\<path to>\vcpkg\scripts\buildsystems\vcpkg.cmake -A x64 ..
    msbuild azure-kust-avro-conv.sln /p:Configuration=Release

## Contributing

This project welcomes contributions and suggestions.  Most contributions require you to agree to a
Contributor License Agreement (CLA) declaring that you have the right to, and actually do, grant us
the rights to use your contribution. For details, visit https://cla.opensource.microsoft.com.

When you submit a pull request, a CLA bot will automatically determine whether you need to provide
a CLA and decorate the PR appropriately (e.g., status check, comment). Simply follow the instructions
provided by the bot. You will only need to do this once across all repos using our CLA.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or
contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.
