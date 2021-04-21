# MPI - Multiprocessing dispatcher

The `mpi` dispatcher runs a pipeline on parallel processes.

## Execution model

### Main pipeline

The term *main pipeline* refers the the root pipeline in the script which is
being ran. For instance, the following script contains two pipelines:

```MPL
| make count=10 showinfo=yes
| foreach [ | output buffer=1 ]
```

* `make count=10 showinfo=yes | foreach` is the main pipeline
* `[ | output buffer=1 ]` is a sub-pipeline

### Processes scheduling

The `mpi` dispacther schedules (creates) new processes when called, and does
not pre-allocate processes when instanciated.

The underlying Python interpreter may schedules new procesess [using three
different methods][mpi_start_methods]:

* `spawn`
* `fork`
* `forkserver`

When using `spawn` and `forkserver`, the new processes does not inherit the
modules which have been dynamically loaded by the parent process. This means
that the new processes will have to load the M42PL at startup, which increases
the startup time; This is why using the `mpi` dispatcher is not recommended
when working in REPL mode.

### Main pipeline layering

All pipeline(s) is/are executed in multiple processes; However:

* The main pipeline is split in into several layers
* The sub-pipelines are not aware of being run if different processes

The main pipeline is split at each command of type `MergingCommand`: this is
necessary since some algorithms / concepts / loops / etc. can be overlly
difficult to run in parallel. For instance the `stats` command injects a the
`PreStats` merging command into its pipeline.

Two layers are usually created:

* The *pre-merging* layer which will be run on several processes
* The *post-merging*` layer which will be run on a single process

### Pipeline layers IPC

The pipeline layers communicates with each other through a Python's 
`multiprocessing`'s `Queue` instance.

---

[mpi_start_methods]: https://docs.python.org/3/library/multiprocessing.html#contexts-and-start-methods
