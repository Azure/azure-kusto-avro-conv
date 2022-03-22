REM Usage .\build.bat <Build Configuration> <VCPKG_DIR> <MSBUILD_DIR> <CMAKE_DIR>
REM Example: .\build.bat Release C:\repos\vcpkg "C:\Program Files (x86)\Microsoft Visual Studio\2019\Enterprise\MSBuild\Current\Bin" "C:\Program Files\CMake\bin"
set Conf=%1
set VCPKG_DIR=%2
set MSBUILD_DIR=%3
set CMAKE_DIR=%4
IF exist build ( echo Build directory exists && cd build) ELSE ( 
mkdir build && echo Build directory created
cd build
%CMAKE_DIR%\cmake.exe -DVCPKG_TARGET_TRIPLET=x64-windows-static -DCMAKE_TOOLCHAIN_FILE=%VCPKG_DIR%\scripts\buildsystems\vcpkg.cmake -A x64 ..
echo Solution and project created
)
%MSBUILD_DIR%\MSBuild.exe azure-kust-avro-conv.sln /p:Configuration=%Conf%
copy %VCPKG_DIR%\installed\x64-windows-release\bin\jemalloc.dll %Conf%\
cd ..