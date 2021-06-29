# M42PL - Dispatchers

Core [M42PL] dispatchers.

M42PL dispatchers runs M42PL pipelines. They are the glue between a pipeline
and an execution framework (single process, multiple processes, Celery, etc.).

## Dispatchers list

| Aliases       | Class                 | Module     | Description                          | Status |
|---------------|-----------------------|------------|--------------------------------------|--------|
| `local`       | `LocalDispatcher`     | `local.py` | Run pipelines (single process)       | Alpha  |
| `local_test`  | `TestLocalDispatcher` | `local.py` | Test pipelines (single process)      | Alpha  |
| `local_repl`  | `REPLLocalDisptcher`  | `local.py` | Run pipelines (single process)       | Alpha  |
| `mpi`         | `MPI`                 | `mpi.py`   | Run pipelines on multiples processes | Alpha  |

## Installation

```Bash
git clone https://github.com/jpclipffel/m42pl-dispatchers
pip install -e m42pl-dispatchers
```

## Usage

One can select the dispatcher to use with the `-d` or `--dispatcher` parameter
when calling `m42pl`.

Examples to use the `mpi` dispatcher:

* To run a pipeline: `m42pl -d mpi run <path/to/pipeline.mpl>`
* In a REPL: `m42pl -d mpi repl`

---

[M42PL]: https://github.com/jpclipffel/m42pl-core
