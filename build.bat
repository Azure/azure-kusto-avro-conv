REM Usage .\build.bat <Build Configuration (Debug|Release|..)> <VCPKG_DIR> <MSBUILD_DIR> <CMAKE_DIR> <MSBUILD_ARGS>
REM Example: .\build.bat Release "C:\repos\vcpkg" "C:\Program Files (x86)\Microsoft Visual Studio\2019\Enterprise\MSBuild\Current\Bin" "C:\Program Files\CMake\bin" -t:Rebuild
set CONF=%1
set VCPKG_DIR=%2
set MSBUILD_DIR=%3
set CMAKE_DIR=%4
set MSBUILD_ARGS=%5
IF exist build ( 
echo Build directory exists
)
ELSE(
mkdir build && echo Build directory created
)
cd build
IF exist %CONF% (
    echo Configuration directory exists
)
ELSE(
mkdir %CONF% && echo Configuration directory created
%CMAKE_DIR%\cmake.exe -DVCPKG_TARGET_TRIPLET=x64-windows-static -DCMAKE_BUILD_TYPE=%CONF% -DCMAKE_TOOLCHAIN_FILE=%VCPKG_DIR%\scripts\buildsystems\vcpkg.cmake -A x64 ..
echo Solution and project created
copy *.sln %CONF%\
copy *.vcxproj %CONF%\
)
%MSBUILD_DIR%\MSBuild.exe %CONF%\azure-kust-avro-conv.sln /p:Configuration=%CONF% %MSBUILD_ARGS%
copy %VCPKG_DIR%\installed\x64-windows-release\bin\jemalloc.dll %CONF%\
cd ..
