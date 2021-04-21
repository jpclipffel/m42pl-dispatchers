# MPI - Local dispatchers

The `local` dispatchers runs a pipeline on a single process.

The following dispatchers are implemented:

* `local`
* `local_test`
* `local_repl`

## Execution model

The pipeline and its sub-pipeline are run as-is, in the `asyncio` loop.
The behaviour of the various local dispatchers changes a little bit:

* `local` simply runs the pipeline
* `local_test` runs the pipeline and store its yield events for further
   processing and test
* `local_repl` add an `output` command ad the end of the pipeline if a simillar
   command (`output`, `noout`) is not already present
