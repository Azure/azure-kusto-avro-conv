azure-kusto-avro-conv
=====================

![Build Status](https://github.com/Azure/azure-kusto-avro-conv/workflows/build/badge.svg)

Utility that converts Avro files to JSON format.


## Building in Linux

### Prerequisites

 * CMake >= 3.12.
 * Private fork of [Apache Avro for C](https://avro.apache.org/docs/current/api/c/index.html)

To install all the required dependencies (in Ubuntu/Debian), run:

    apt-get install libjansson-dev liblzma-dev libsnappy-dev zlib1g-dev libgmp-dev pkg-config

Build private Avro C fork that includes logical types support:

    git clone https://github.com/spektom/avro.git
    cd avro/lang/c
    git checkout c_logical_types
    mkdir build
    cd build
    cmake ..
    make -j

### Compiling

    mkdir build
    cd build
    cmake -DAVRO_LIBRARY=../../avro/lang/c/build/src/libavro.a -DAVRO_INCLUDE_DIR=../../avro/lang/c/src ..
    make -j

## Building in Windows

### Prerequisites

 * Microsoft Visual Studio 2019/2021.
 * CMake >= 3.12.
 * [Private fork](https://github.com/spektom/vcpkg/tree/avro_logical_types) of VCPKG.
 * Apache Avro installed with `vcpkg` (see below).
 * GMP library (mpir) installed with `vcpkg` (see below).
 * Jemalloc library installed with `vcpkg` (see below).

To install Apache Avro library, run:

    git clone https://github.com/spektom/vcpkg.git
    cd vcpkg
    git checkout avro_logical_types
    .\bootstrap-vcpkg.bat
    .\vcpkg install avro-c:x64-windows-static
    .\vcpkg install mpir:x64-windows-static
    .\vcpkg install jemalloc:x64-windows-release

### Compiling

    .\build.bat <Build Configuration> <VCPKG_DIR> <MSBUILD_DIR> <CMAKE_DIR>

    Example: .\build.bat Release C:\repos\vcpkg "C:\Program Files (x86)\Microsoft Visual Studio\2019\Enterprise\MSBuild\Current\Bin" "C:\Program Files\CMake\bin"

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
