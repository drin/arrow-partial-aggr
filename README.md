# Overview

A repository to investigate partial aggregates using Aparche Arrow's compute API.


# Source files

The repository should be organized in a straightforward manner. All of the C++ source and header
files are in `src`, test data and the pacman build file (`PKGBUILD`) are in `resources`, and all
other files are in the root directory.

## LFS

The test data is in git LFS because I thought that may be convenient (apologies if it isn't). So,
be sure to get the test data after checking out the repository:

```bash
# downloads the file data from LFS
git lfs checkout "resources/E-GEOD-76312.48-2152.x565.feather"
```


# Compilation

This repository uses [meson][web-meson] to compile. This is partially because I don't understand
CMake very well, and partially because I have found meson to be extremely easy to learn and use.

I am using meson 0.61.2, and it is easy to install into a python environment. I am not sure that
system packages on non rolling-release distros will have the latest version, but this particular
meson build file, `meson.build`, is *extremely* simple and should be compatible with older
versions. Note that meson uses ninja, so it is likely you will need to install ninja before meson,
and that familiarity with ninja will make some meson commands easier to understand.

To actually build the code with meson, here are some simple bootstrap instructions:

```bash
# creates a directory, called `build-dir`
meson setup build-dir

# command to compile; the `-C` flag says to "change to the given directory and run the command"
# I think this is essentially ninja syntax
meson compile -C build-dir
```

Note that the ">> Dependencies" section of the meson.build file specifies our only "real"
dependency, Apache Arrow 6.0.1 (though, I don't have any reason to believe that 7.0.0 would be
difficult to use, I just haven't tried it).


# Execution

The compilation section, above, describes how to compile the source files (`ops.cpp` and
`simple.cpp`) into executable binaries. The ">> Executables" section defines the executables that
will be created; specifically, we expect the executable `simple` to exist after successful
compilation:

```bash
# Execute the binary so that it runs the aggregation function on the test data, in memory as a
# single table
time ./build-dir/simple "$PWD" table

# Execute the binary so that it applies the aggregation function on every "chunk" of the table
# as a separate table. We don't time any of the conversion, just the compute portion.
time ./build-dir/simple "$PWD" vector
```


# Performance

I am interested in partial aggregates and applying them on distinct devices, environments, or on
data that is retrieved from distinct devices and environments.

## System Info

I am running on ArchLinux:
```bash
>> uname -a
Linux <hostname> 5.16.10-arch1-1 #1 SMP PREEMPT Wed, 16 Feb 2022 19:35:18 +0000 x86_64 GNU/Linux
```

I installed Arrow by using a modified version of the PKGBUILD file available in the `resources/`
directory (original available here: [PKGBUILD][archgit-arrow-6.0.1]). Some key information from it
(IMO):
```
  cmake                                                \
    -B build -S apache-${pkgname}-${pkgver}/cpp        \
    ...
    -DCMAKE_BUILD_TYPE=Release                         \
    -DARROW_DEPENDENCY_SOURCE=SYSTEM                   \
    -DARROW_BUILD_STATIC=ON                            \
    -DARROW_BUILD_SHARED=ON                            \
    -DARROW_COMPUTE=ON                                 \
    -DARROW_FLIGHT=ON                                  \
    -DARROW_GANDIVA=ON                                 \
    -DARROW_IPC=ON                                     \
    -DARROW_PYTHON=ON                                  \
    -DARROW_SIMD_LEVEL=AVX2                            \
    ...                                                \
  make -C build
```

My gcc and LLVM versions:
```bash
>> g++ --version
gcc (GCC) 11.2.0

>> clang++ --version
clang version 13.0.1
Target: x86_64-pc-linux-gnu
Thread model: posix
```

Some processor info (I don't know how SSE compares to AVX2):
```bash
>> dmidecode -t 4
# ...
	SSE (Streaming SIMD extensions)
	SSE2 (Streaming SIMD extensions 2)
# ...
Version: Intel(R) Xeon(R) CPU E3-1270 v3 @ 3.50GHz
Core Count: 4
Core Enabled: 4
Thread Count: 8
# ...
```


## Results
For the first table-based scenario, the timing I see is as follows:

```bash
# Note: "..." is used in place of the path of my home directory for this output

>>  time ./build-dir/simple "$PWD" table
Using directory  [.../code/misc/cpp/arrow-compute-perf]
Aggregating over [table]
Parsing file: 'file://.../code/misc/cpp/arrow-compute-perf/resources/E-GEOD-76312.48-2152.x565.feather'
Reading arrow IPC-formatted file: file://.../code/misc/cpp/arrow-compute-perf/resources/E-GEOD-76312.48-2152.x565.feather
Table dimensions [27118, 2152]
        [565] chunks
        [48] chunk_size
Aggr Time:      2240ms

________________________________________________________
Executed in    4.08 secs    fish           external
   usr time    3.81 secs  500.00 micros    3.81 secs
   sys time    0.28 secs   98.00 micros    0.28 secs
```


For the second "slice-based" or "vector-based" approach, the timing I see is as follows:

```bash
# Note: "..." is used in place of the path of my home directory for this output

>> time ./build-dir/simple "$PWD" table
Using directory  [.../code/misc/cpp/arrow-compute-perf]
Aggregating over [table]
Parsing file: 'file://.../code/misc/cpp/arrow-compute-perf/resources/E-GEOD-76312.48-2152.x565.feather'
Reading arrow IPC-formatted file: file://.../code/misc/cpp/arrow-compute-perf/resources/E-GEOD-76312.48-2152.x565.feather
Table dimensions [27118, 2152]
        [565] chunks
        [48] chunk_size
Aggr Time:      55514ms

________________________________________________________
Executed in   58.67 secs    fish           external
   usr time   58.30 secs  502.00 micros   58.30 secs
   sys time    0.30 secs   96.00 micros    0.30 secs
```


<!-- resources -->
[web-meson]:           https://mesonbuild.com/
[archgit-arrow-6.0.1]: https://github.com/archlinux/svntogit-community/blob/packages/arrow/trunk/PKGBUILD
