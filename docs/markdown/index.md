# M42PL - Dispatchers

* [Local](./local.md)
* [Multiprocessing](./mpi.md)

## Write a dispatcher

A dispatcher is a functor which inherits from `m42pl.dispatcher.Dispatcher`.
A simple example is given and explained hereafter:

```Python
import asyncio
from m42pl.dispatchers import Dispatcher


class MyDispatcher(Dispatcher):
    _aliases_ = ['mydispatcher',]

    def target(self, context, event):
        async def run():
            async with context.kvstore:
                pipeline = context.pipelines['main']
                async for _ in pipeline(context, event):
                    pass
        asyncio.run(run())
```

Lets disect this code step by step:

```Python
import asyncio
from m42pl.dispatchers import Dispatcher
```

The first two lines import `asyncio` which is needed to run a pipeline and 
`Dispatcher` which is M42PL's base dispatcher class to inherit from.

```Python
class MyDispatcher(Dispatcher):
    _aliases_ = ['mydispatcher',]
```

A dispatcher is a class which should inherits from the `Dispatcher` base class.
It should also provide at least one *alias*, which the dispatcher friendly name
to be used when running M42PL: `m42pl run -d mydispatcher ...`.

Of course one can provide more than one alias.

```Python
class MyDispatcher(Dispatcher):
    _aliases_ = ['mydispatcher',]

    def target(self, context, event):
        # ...
```

The `target` method is called automatically by M42PL. It receives a `context`
and an `event` as parameters:

* `context` is an object which contains the current pipeline and its
  sub-pipelines & the key/value store interface.
* `event` is the initial event, which may be empty

```Python
class MyDispatcher(Dispatcher):
    _aliases_ = ['mydispatcher',]

    def target(self, context, event):
        async def run():
            async with context.kvstore:
                pipeline = context.pipelines['main']
                async for _ in pipeline(context, event):
                    pass
        asyncio.run(run())
```

The `target` method should schedules (calls) an asynchronous function or method
to run the pipeline (which is itself an functor behaving like an asynnchronous
generator).

The `run` nested function (which also be a method) starts by entring the
`context`'s key/value store with `async with`; this ensure the key/value store
is properly initalizaed and de-initialized.

The main pipeline is then ran in an `async for` loop.

## Tips

### Rebuild the pipeline after changes

The dispatchers can change a pipeline structure by adding, removing and
splitting it. This is an expected behaviour as the dispaters must adapt the
pipelines to the underlying execution framework.

If you implement a dispatcher which change the pipeline structure, do not
forget to run `.build()` on the updated pipeline to gurantee its internal
coherence.

---
