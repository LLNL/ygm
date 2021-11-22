# What is YGM?
YGM is a general-purpose, pseudo-asynchronous communication library built on top of MPI in C++. YGM's utility is
provided through its mailbox abstraction which are used for point-to-point and broadcast communications. When using YGM,
individual cores queue messages into a mailbox instead of
directly sending them.

YGM is an asynchronous communication library designed for irregular communication patterns. It is built on a
communicator abstraction, much like MPI, but communication is handled asynchronously and is initiated by senders without
any interaction with receivers. YGM features
* **Message buffering** - Increases application throughput.
* **Fire-and-Forget RPC Semantics** - A sender provides the function and function arguments for execution on a specified
  destination rank through an `async` call. This function will complete on the destination rank at an unspecified time
  in the future, but YGM does not explicitly make the sender aware of this completion.
* **Pre-built Storage Containers** - YGM provides a collection of distributed storage containers with asynchronous
  interfaces, used for many common distributed memory operations. Containers are designed to partition data, allowing
insertions to occur from any rank. Data is accessed through collective `for_all` operations that execute a user-provided
function on every stored object, or, when a particular piece of data's location is known, `visit`-type operations that
perform a user-provided function only on the desired data. These containers are found
[here](/include/ygm/containers/)

# Getting Started

## Requirements
* C++17 - GCC versions 9+ are tested. Your mileage may vary with other compilers.
* [Cereal](https://github.com/USCiLab/cereal) - C++ serialization library
* MPI

## Using YGM with CMake
YGM is a header-only library that is easy to incorporate into a project through CMake. Adding the following to
CMakeLists.txt will install YGM and its dependencies as part of your project:
```
find_package(ygm CONFIG)
if(NOT ygm_FOUND)
    FetchContent_Declare(
        ygm
        GIT_REPOSITORY https://github.com/LLNL/ygm
        GIT_TAG        <commit hash here>         
    )         
    set(JUST_INSTALL_YGM ON)
    set(YGM_INSTALL ON)
    FetchContent_MakeAvailable(ygm)
    message(STATUS "Cloned ygm dependency " ${ygm_SOURCE_DIR})
else()
    message(STATUS "Found ygm dependency " ${ygm_DIR})
endif()
target_link_libraries(my_cool_project INTERFACE ygm::ygm)
```

# Potential Pitfalls

## Allowed Lambdas
There are two distinct classes of lambdas that can be given to YGM: *remote lambdas* and *local lambdas*, each of which
has different requirements.

### Remote Lambdas
A *remote lambda* is any lambda that may potentially be executed on a different rank. These lambdas are identified as
being those given to a `ygm::communicator` or any of the storage containers through a function prefixed by `async`.

The defining feature of remote lambdas is they **must not** capture any variables; all variables must be provided as
arguments. This limitation is due to the lack of
ability for YGM to inspect and extract these arguments when serializing messages to be sent to other ranks.

### Local Lambdas
A *local lambda* is any lambda that is guaranteed not to be sent to a remote rank. These lambdas are identified as being
those given to a `for_all` operation on a storage container.

The defining feature of local lambdas is that all arguments besides what is stored in the container must be captured.
Internally, these lambdas are given to a [`std::for_each`](https://en.cppreference.com/w/cpp/algorithm/for_each) that
iterates over the container's elements stored locally on each rank.

# License
YGM is distributed under the MIT license.

All new contributions must be made under the MIT license.

See [LICENSE-MIT](LICENSE-MIT), [NOTICE](NOTICE), and [COPYRIGHT](COPYRIGHT) for
details.

SPDX-License-Identifier: MIT

# Release
LLNL-CODE-789122
