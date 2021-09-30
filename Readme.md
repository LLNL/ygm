# What is YGM ?
YGM is a general-purpose, pseudo-asynchronous communication library built on top of MPI in C++. YGM's utility is
provided through its mailbox abstraction which are used for point-to-point and broadcast communications. When using YGM,
individual cores queue messages into a mailbox instead of
directly sending them.


# Using YGM
* Place headers in an accessible location
* Include the appropriate headers
* Initialize YGM
* Create a mailbox

See the [mailbox_test](src/mailbox_test.cpp) for an example.

# Building Examples & Tests with Spack
```bash
$ mkdir build
$ cd build
$ spack load gcc@9 # or newer
$ cmake ../
$ make
$ salloc -N1 -ppdebug # if testing in a Slurm environment
$ make test
```



# License
YGM is distributed under the MIT license.

All new contributions must be made under the MIT license.

See [LICENSE-MIT](LICENSE-MIT), [NOTICE](NOTICE), and [COPYRIGHT](COPYRIGHT) for
details.

SPDX-License-Identifier: MIT

# Release
LLNL-CODE-789122
