import os
import multiprocessing
from multiprocessing import Pipe, Process, get_logger
import asyncio

from typing import List, Any

import m42pl
from m42pl.context import Context
from m42pl.pipeline import Pipeline
from m42pl.dispatchers import Dispatcher
from m42pl.commands import MergingCommand


def run_pipeline(pipeline_dc: dict, context_dc: dict, first: bool, last: bool, cin: Any, cout: Any, chunk: int, chunks: int, background: bool, modules_paths: list = [], modules_names: list = []):
    """Runs a M42PL pipeline.
    
    A new command `m42pl_mpi_send` is added to the pipeline before its execution
    in order to forward its events to the `connection` pipe.
    
    :param pipeline_dc:     Serialized pipeline
    :param context_dc:      Serialized context
    :param first:           True if first pipeline in a batch, False otherwise
    :param last:            True if last pipeline in a batch, False otherwise
    :param cin:             Multiprocessing pipe input (read from)
    :param cout:            Multiprocessing pipe output (write to)
    :param chunk:           Instance chunk count
    :param chunks:          Total number of chunks
    :param background:      True if the process runs in background, False otherwise
    :param modules_paths:   Modules to load, specified by path (e.g. `/path/to/module.py`)
    :param modules_names:   Modules to load, specified by name (e.g. `some.module`)
    """

    async def _run(pipeline) -> None:
        async for _ in pipeline():
            pass

    if background and os.fork() != 0:
        return
    # Load requested modules.
    m42pl.load_modules(paths=modules_paths, names=modules_names)
    # Build local context from serialized context
    context = Context.from_dict(context_dc)
    # Build local pipeline from serialized pipeline
    pipeline = Pipeline.from_dict(pipeline_dc)
    # Setup pipeline start and end commands
    if not first:
        pipeline.prepend_commands([ m42pl.command('m42pl_mpi_receive')(cin), ])
    if not last:
        pipeline.append_commands([ m42pl.command("m42pl_mpi_send")(cout), ])
    # Setup pipeline chunks
    pipeline.set_chunk(chunk, chunks)
    # Setup pipeline context
    pipeline.context = context
    # Run pipeline
    asyncio.run(_run(pipeline))


class MPI(Dispatcher):
    """Run pipelines in mutliple parallels processes (**not** threads).
    """

    _aliases_ = ['multiprocessing', 'mpi']

    def __init__(self, context: Context, background: bool = False, max_cpus: int = 0, method: str = None) -> None:
        """
        :param context:     M42PL context.
        :param background:  True if the processes must be detached,
                            False otherwise. Defaults to False.
        :param max_cpus:    Maximum number of CPU to use.
                            The effective number of parallel pipeline
                            is `max_cpus - 1` since a default process
                            is spawned to gather events. Defaults to
                            `multiprocessing.cpu_count() - 1`.
        :param method:      Multiprocessing start method (`fork`,
                            `forkserver` or `spawn`).
                            If `None`, use default start method.
                            Defaults to `None`.
        """
        super().__init__(context)
        self.background = background
        self.max_cpus = max_cpus > 0 and max_cpus or multiprocessing.cpu_count()
        if method:
            multiprocessing.set_start_method(method.lower())
        self.processes = []
        self.pids = [] # type: List[int]
        # ---
        # Support for all multiprocessing start methods
        #
        # When using 'spwan' and 'forkserver', dynamically loaded
        # modules (by path or name) are not copied to the new Python
        # process. We need to reload them in the new processes.
        if multiprocessing.get_start_method() != 'fork':
            modules_refs = {
                'modules_paths': m42pl.IMPORTED_MODULES_PATHS,
                'modules_names': m42pl.IMPORTED_MODULES_NAMES
            }
        else:
            modules_refs = {}


        _, main_pipeline = list(self.context.pipelines.items())[-1]
        

        pipelines = main_pipeline.split_by_type(
            cmd_type=MergingCommand,
            cmd_single=False
        )
        
        for i, pipeline in enumerate(pipelines):
            for cpu_count in range(1, self.max_cpus + 1):

                if i == 0:
                    cin, cout = multiprocessing.Pipe()
                else:
                    cin = cout
                    cout, cin = multiprocessing.Pipe()

                self.processes.append(Process(
                    target=run_pipeline,42
                    kwargs={
                        **{
                            'pipeline_dc': pipeline.to_dict(),
                            'context_dc': context.to_dict(),
                            'first': i == 0,
                            'last': i == len(pipelines) - 1,
                            'cin': i == 0 and None or cin,
                            'cout': i == len(pipelines) - 1 and cout or None,
                            'chunk': cpu_count,
                            'chunks': self.max_cpus,
                            'background': self.background
                       },
                        **modules_refs
                    }
                ))






        # print(sub_pipelines)

        # pipes = []
        # for i, pipeline in enumerate(pipelines):
        #     if not len(pipes):
        #         pipes = list(multiprocessing.Pipe())
        #         # cin, cout = multiprocessing.Pipe()
        #     for cpu_count in range(0, self.max_cpus):
        #         self.processes.append(Process(
        #             target=run_pipeline,
        #             kwargs={
        #                 **{
        #                     'js': pipeline.to_dict(),
        #                     'first': i == 0,
        #                     'last': i == len(pipelines) - 1,
        #                     'cin': i == 0 and None or pipes.pop(),
        #                     'cout': i == len(pipelines) - 1 and None or pipes.pop(),
        #                     'chunk': cpu_count + 1,
        #                     'chunks': max_cpus,
        #                     'background': self.background
        #                 },
        #                 **modules_refs
        #             }
        #         ))
        # # ---
        # # Prepare processes
        # for i, pipeline in enumerate(pipelines):
        #     for cpu_count in range(0, self.max_cpus):
        #         self.processes.append(Process(
        #             target=run_pipeline,
        #             kwargs={
        #                 **{
        #                     'js': pipeline.to_dict(),
        #                     'first': i == 0,
        #                     'last': i == len(pipelines) - 1,
        #                     'cin': cin,
        #                     'cout': cout,
        #                     'chunk': cpu_count + 1,
        #                     'chunks': max_cpus,
        #                     'background': self.background
        #                 }, **modules_refs}
        #         )) 


        # # ---
        # # Setup paralel pipelines processes.
        # main_pipeline = pipeline.split_from()
        # for cpu_count in range(0, self.max_cpus):
        #     self.processes.append(
        #         Process(target=run_main_pipeline, kwargs={
        #             **{
        #                 "js": pipeline.to_dict(),
        #                 "connection": cnx_out,
        #                 "chunk": cpu_count + 1,
        #                 "chunks": max_cpus,
        #                 "background": self.background
        #             }, **modules_refs}))
        # # ---
        # # Setup gathering process.
        # self.processes.append(
        #     Process(target=run_terminal_pipeline, kwargs={
        #         **{
        #             "connection": cnx_in,
        #             "background": self.background
        #         }, **modules_refs}))

    def to_dict(self):
        return {'pids': self.pids}

    def __call__(self):
        for process in self.processes:
            process.start()
        self.pids = [ p.pid for p in self.processes ]
        if not self.background:
            for process in self.processes:
                process.join()
        else:
            return self.pids
