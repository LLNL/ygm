# What is YGM?

YGM is an asynchronous communication library designed for irregular communication patterns. It is built on a
communicator abstraction, much like MPI, but communication is handled asynchronously and is initiated by senders without
any interaction with receivers. YGM features
* **Message buffering** - Increases application throughput.
* **Fire-and-Forget RPC Semantics** - A sender provides the function and function arguments for execution on a specified
  destination rank through an `async` call. This function will complete on the destination rank at an unspecified time
  in the future, but YGM does not explicitly make the sender aware of this completion.
* **Storage Containers** - YGM provides a collection of distributed storage containers with asynchronous
  interfaces, used for many common distributed memory operations. Containers are designed to partition data, allowing
insertions to occur from any rank. Data is accessed through collective `for_all` operations that execute a user-provided
function on every stored object, or, when a particular piece of data's location is known, `visit`-type operations that
perform a user-provided function only on the desired data. These containers are found
[here](/include/ygm/container/).

# Getting Started

## Requirements
* C++17 - GCC versions 8, 9 and 10 are tested. Your mileage may vary with other compilers.
* [Cereal](https://github.com/USCiLab/cereal) - C++ serialization library
* MPI
* Optionally, Boost 1.77 to enable Boost.JSON support.  


## Using YGM with CMake
YGM is a header-only library that is easy to incorporate into a project through CMake. Adding the following to
CMakeLists.txt will install YGM and its dependencies as part of your project:
```
set(DESIRED_YGM_VERSION 0.4)
find_package(ygm ${DESIRED_YGM_VERSION} CONFIG)
if (NOT ygm_FOUND)
    FetchContent_Declare(
        ygm
        GIT_REPOSITORY https://github.com/LLNL/ygm
        GIT_TAG v${DESIRED_YGM_VERSION}
    )
    FetchContent_GetProperties(ygm)
    if (ygm_POPULATED)
        message(STATUS "Found already populated ygm dependency: "
                       ${ygm_SOURCE_DIR}
        )
    else ()
        set(JUST_INSTALL_YGM ON)
        set(YGM_INSTALL ON)
        FetchContent_Populate(ygm)
        add_subdirectory(${ygm_SOURCE_DIR} ${ygm_BINARY_DIR})
        message(STATUS "Cloned ygm dependency " ${ygm_SOURCE_DIR})
    endif ()
else ()
    message(STATUS "Found installed ygm dependency " ${ygm_DIR})
endif ()
```

# Anatomy of a YGM Program
Here we will walk through a basic "hello world" YGM program. The [examples directory](/examples/) contains several other
examples, including many using YGM's storage containers.

To begin, headers for a YGM communicator are needed
``` C++
#include <ygm/comm.hpp>
```

At the beginning of the program, a YGM communicator must be constructed. It will be given `argc` and `argv` like
`MPI_Init`, and it has an optional third argument that specifies the aggregate size (in bytes) allowed for all send
buffers before YGM begins flushing sends. Here, we will make a buffer with 32MB of aggregate send buffer space.
``` C++
ygm::comm world(&argc, &argv, 32*1024*1024);
```

Next, we need a lambda to send through YGM. We'll do a simple hello\_world type of lambda.
``` C++
auto hello_world_lambda = [](const std::string &name) {
	std::cout << "Hello " << name << std::endl;
};
```

Finally, we use this lambda inside of our `async` calls. In this case, we will have rank 0 send a message to rank 1,
telling it to greet the world
``` C++
if (world.rank0()) {
	world.async(1, hello_world_lambda, std::string("world"));
}
```

The full, compilable version of this example is found [here](/examples/hello_world.cpp). Running it prints a single
"Hello world".

# Potential Pitfalls

## Allowed Lambdas
There are two distinct classes of lambdas that can be given to YGM: *remote lambdas* and *local lambdas*, each of which
has different requirements.

### Remote Lambdas
A *remote lambda* is any lambda that may potentially be executed on a different rank. These lambdas are identified as
being those given to a `ygm::comm` or any of the storage containers through a function prefixed by `async_`.

The defining feature of remote lambdas is they **must not** capture any variables; all variables must be provided as
arguments. This limitation is due to the lack of
ability for YGM to inspect and extract these arguments when serializing messages to be sent to other ranks.

### Local Lambdas
A *local lambda* is any lambda that is guaranteed not to be sent to a remote rank. These lambdas are identified as being
those given to a `for_all` operation on a storage container.

The defining feature of local lambdas is that all arguments besides what is stored in the container must be captured.
Internally, these lambdas may be given to a [`std::for_each`](https://en.cppreference.com/w/cpp/algorithm/for_each) that
iterates over the container's elements stored locally on each rank.

# License
YGM is distributed under the MIT license.

All new contributions must be made under the MIT license.

See [LICENSE-MIT](LICENSE-MIT), [NOTICE](NOTICE), and [COPYRIGHT](COPYRIGHT) for
details.

SPDX-License-Identifier: MIT

# Release
LLNL-CODE-789122
