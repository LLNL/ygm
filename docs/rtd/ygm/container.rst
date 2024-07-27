.. _ygm-container:

:code:`ygm::container` module reference.
========================================

:code:`ygm::container` is a collection of distributed containers designed specifically
to perform well within YGM's asynchronous runtime.
Inspired by C++'s Standard Template Library (STL), the containers provide
improved programmability by allowing developers to consider an algorithm as the
operations that need to be performed on the data stored in a container while
abstracting the locality and access details of said data.
While insiration is taken from STL, the top priority is to provide expressive
and performant tools within the YGM framework.
Interaction with containers occurs in one of two classes of operations:
:code:`for_all` and `async_visit`.

Both classes expect a function as a primary argument, similar to
:code:`ygm::comm::async`.
However, the passed function signature must match the contents of the container.
Value store containers holding :code:`value_type` objects expect the first
argument of passed functions to address objects with the syntax
:code:`[](value_type &data_item){}`.
Key-value store containers expect these functions instead to support separate
:code:`key_type` (which must be immutable) and :code:`value_type` arguments with
the syntax :code:`[](key_type key, value_type &value){}`.
Although all of these operations agree as to how contained objects are addressed
by functions, the interfaces are subtly different and support additional
optional features.

:code:`for_all`-class operations are barrier-inducing collectives that direct
ranks to iteratively apply the passed function to all locally-held data.
Functions passed to the :code:`for_all` interface do not support additional
variadic parameters.
However, these functions are stored and executed locally on each rank, and so
can capture objects in rank-local scope.

:code:`async_visit`-class operations provide a mechanism for executing a
function at a particular piece of data stored within a container.
YGM handles the creation and invocation of a YGM communicator :code:`async`
call, freeing the user to consider algorithmic details.
Not all containers support :code:`async_visit`-class operations.

.. toctree::
   :maxdepth: 2
   :caption: Container Classes:

   container/array
   container/bag
   container/counting_set
   container/disjoint_set
   container/map
   container/multimap
   container/multiset
   container/set

YGM also supports adaptor classes and functions that wrap an existing class to
either add or modify operation functionality.

.. doxygenfunction:: ygm::container::reduce_by_key_map