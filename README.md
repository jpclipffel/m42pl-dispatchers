# M42PL - Dispatchers

Core [M42PL](https://github.com/jpclipffel/m42pl-core) dispatchers.

## What are M42PL dispatchers ?

M42PL dispatchers executes M42PL pipelines. They are the glue between
a pipeline and an execution framework (single process, fork server,
Celery, etc.).

## Installation

```shell
git clone https://github.com/jpclipffel/m42pl-dispatchers
pip install -e m42pl-dispatchers
```

## Dispatchers list

| Aliases                  | Class                 | Module     | Description                          | Status           |
|--------------------------|-----------------------|------------|--------------------------------------|------------------|
| `local`                  | `LocalDispatcher`     | `local.py` | Run a pipeline locally               | Alpha            |
| `local_test`             | `TestLocalDispatcher` | `local.py` | Test pipeline locally                | Alpha            |
| `local_shell`            | `ShellLocalDisptcher` | `local.py` | Run pipelines in a Shell-like CLI    | Alpha            |
| `multiprocessing`, `mpi` | `MPI`                 | `mpi.py`   | Run pipelines on multiples processes | Work in progress |
