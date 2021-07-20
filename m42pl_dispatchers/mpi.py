import os
import json
import multiprocessing
from multiprocessing import Process, Queue
import asyncio

from typing import List

import m42pl
from m42pl.context import Context
from m42pl.pipeline import Pipeline
from m42pl.dispatchers import Dispatcher
from m42pl.commands import MergingCommand


# Get number of usable CPUs
# On Linux, use `os.sched_getaffinity(0)`
# On other OS, use `os.cpu_count()`
try:
    # pylint: disable=no-member
    MAX_CPUS = len(os.sched_getaffinity(0))
except Exception:
    MAX_CPUS = os.cpu_count()


def run_pipeline(context: str, event: str, chan_read: Queue,
                    chan_write: Queue, chunk: int, chunks: int,
                    modules: dict):
    """Runs a split pipeline.

    :param context:     Source context as JSON string
    :param event:       Source event as JSON string
    :param chan_read:   Multiprocessing queue output (read from);
    :param chan_write:  Multiprocessing queue input (write to);
    :param chunk:       Current chunk number (starts at 0)
    :param chunks:      Total number of chunks (i.e. number of parallel
                        split pipelines / process)
    :param modules:     Modules names and paths to load
    """

    async def run(pipeline, context, event):
        async with context.kvstore:
            async for _ in pipeline(context, event):
                pass

    # Load missing modules
    m42pl.load_modules(names=modules['names'], paths=modules['paths'])
    # Build local context and event from seralized instances
    context = Context.from_dict(json.loads(context))
    # event = Event.from_dict(json.loads(event))
    event = json.loads(event)
    # Get main pipeline
    pipeline = context.pipelines['main']
    # ---
    # Customize main pipeline to read & write to the multiprocessing pipe
    # Intermediate or last command: read input from MPI pipe
    if chan_read:
        pipeline.commands = [
            m42pl.command('mpi-receive')(chan_read),
        ] + pipeline.commands
    # First or intermediate command: write output to MPI pipe
    if chan_write:
        pipeline.commands.append(m42pl.command('mpi-send')(chan_write))
    # ---
    # Rebuild, reconfigure and run pipeline
    pipeline.build()
    pipeline.set_chunk(chunk, chunks)
    asyncio.run(run(pipeline, context, event))


class MPI(Dispatcher):
    """Run pipelines in mutliple parallels processes (**not** threads).

    This dispatcher is not recomended for REPL application (although
    functionnal) as it `fork()` each time it is called.

    TODO: Create a variant (e.g. `REPLMPI`) which will be more suitable
    for REPL application.

    :ivar processes:    List of pipelines processes
    :ivar modules:      M42PL modules paths and names
    """

    _aliases_ = ['multiprocessing', 'mpi']

    def __init__(self, background: bool = False, max_cpus: int = 0,
                    method: str = None, *args, **kwargs):
        """
        :param background:  True if the processes must be detached,
                            False otherwise. Defaults to False
        :param max_cpus:    Maximum number of CPU to use; Defaults to
                            number of CPU
        :param method:      Multiprocessing start method (`fork`,
                            `forkserver` or `spawn`);
                            If `None`, use default start method;
                            Defaults to `None`
        """
        super().__init__(*args, **kwargs)
        self.background = background
        self.max_cpus = max_cpus or MAX_CPUS
        self.method = method
        if self.method:
            multiprocessing.set_start_method(method.lower())
        self.processes = []
        # When using 'spwan' and 'forkserver', dynamically loaded
        # modules (by path or name) are not copied to the new Python
        # process. We need to reload them in the new processes.
        if multiprocessing.get_start_method() != 'fork':
            self.modules = {
                'paths': m42pl.IMPORTED_MODULES_PATHS,
                'names': m42pl.IMPORTED_MODULES_NAMES
            }
        else:
            self.modules = {
                'paths': [],
                'names': []
            }

    def split_pipeline(self, pipeline: Pipeline,
                        max_layers: int = 2) -> List[Pipeline]:
        """Splits the :param:`pipeline` by command type.

        The source :param:`pipeline` is split at each merging command
        and in at most :param:`max_layers` layers.

        :param pipeline:    Source pipeline
        :param max_layers:  Maximum number of layers; Default to 2
                            (one for pre-merging commands and one for 
                            merging and post-merging commands)
        """
        commands = [[],]
        pipelines = []
        # Build the new pipelines' commands lists
        for cmd in pipeline.commands:
            # Split when the command type is a `MergingCommand`
            if isinstance(cmd, MergingCommand) and len(commands) < max_layers:
                commands.append([cmd,])
            # Append non-merging command to current commands list
            else:
                commands[-1].append(cmd)
        # Build and returns new pipelines
        for cmds in commands:
            pipelines.append(Pipeline(
                commands=cmds,
                name=f'{pipeline.name}',
                # timeout=pipeline.timeout,
            ))
        return pipelines

    def target(self, context, event):
        # ---
        # Get main pipeline
        main_pipeline = context.pipelines['main']
        # ---
        # Split main pipeline
        layers = self.split_pipeline(main_pipeline)
        # ---
        # Create processes
        for layer, pipeline in enumerate(layers):
            # Setup channels for first layer
            if layer == 0:
                chan_read = None
                chan_write = Queue()
            # Setup channels for intermediate layer
            elif layer > 0 and layer < (len(layers) - 1):
                chan_read = chan_write
                chan_write = Queue()
            # Setup channels for last layer
            else:
                chan_read = chan_write
                chan_write = None
            # Setup chunks count
            # `chunks` is the number of parallels pipelines to run.
            # If the `pipeline_count` is:
            # * even : non-merging pipeline => dispatch on `max_cpu` processes
            # * non-even : merging pipeline => dispatch on a single process
            chunks = layer % 2 == 0 and self.max_cpus or 1
            # Amend context
            context.pipelines['main'] = pipeline
            # Setup layer's processes
            for chunk in range(0, chunks):
                self.processes.append(Process(
                    target=run_pipeline,
                    kwargs={
                        'context': json.dumps(context.to_dict()),
                        # 'event': json.dumps(event.to_dict()),
                        'event': json.dumps(event),
                        'chan_read': chan_read,
                        'chan_write': chan_write,
                        'chunk': chunk,
                        'chunks': chunks,
                        'modules': self.modules
                    }
                ))
        # ---
        # Start processes
        for process in self.processes:
            process.start()
        # ---
        # Join processes
        for process in self.processes:
            process.join()
        # ---
        # Cleanup
        self.processes = []
