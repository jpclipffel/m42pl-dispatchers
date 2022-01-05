from __future__ import annotations

import os
import asyncio
from pathlib import Path
import multiprocessing
# from pathos.multiprocessing import ProcessPool
import dill
import psutil

from m42pl.pipeline import PipelineRunner
from m42pl.dispatchers import Dispatcher
import m42pl.errors

from m42pl.encoders import ALIASES as encoders_aliases


class LocalDispatcher(Dispatcher):
    """Runs pipelines in the current process.
    """

    _aliases_ = ['local',]

    def __init__(self, workdir: str = '.', uv: bool = False,
                    *args, **kwargs) -> None:
        """
        :param workdir:     Working directory
        :param uv:          Use uvloop or not; Default to `False`
        :param timeout:     Pipeline's timeout
        """
        super().__init__(*args, **kwargs)
        self.workdir = Path(workdir)
        self.use_uv = uv
        # ---
        if not self.workdir.is_dir():
            raise m42pl.errors.DispatcherError(
                self, 
                f'Requested workdir does not exists: workdir="{workdir}"'
            )
        if self.use_uv:
            import uvloop
            uvloop.install()
            self.logger.info('installed uvloop')
        # ---
        self.current_kvstore = None
        self.current_identifier = None

    async def __aexit__(self, *args, **kwargs):
        await self.unregister(self.current_kvstore, self.current_identifier)

    async def _run(self, context, event) -> None:
        """Run the context main pipeline.
        """
        async with context.kvstore:
            async with self:
                self.current_kvstore = context.kvstore
                self.current_identifier = os.getpid()
                # Write pipeline / process ID to KVStore
                # await self.register(context.kvstore, os.getpid())
                await self.register(self.current_kvstore, self.current_identifier)
                # Select and run pipeline
                # try:
                pipeline = context.pipelines['main']
                runner = PipelineRunner(pipeline)
                # async for _ in pipeline(context, event):
                async for _ in runner(context, event):
                    pass
                # except (Exception, StopAsyncIteration):
                #     # Remove pipeline / process ID from KVStore
                #     await self.unregister(context.kvstore, os.getpid())
                #     raise
                # # Remove pipeline / process ID from KVStore
                # await self.unregister(context.kvstore, os.getpid())

    def target(self, context, event):
        os.chdir(self.workdir)
        asyncio.run(self._run(context, event))


class TestLocalDispatcher(LocalDispatcher):
    """A dispatcher to be used for local tests.

    This dispatcher stores the pipeline results in a list and
    returns this list atfer the pipeline execution.
    
    **This dispatcher is not suitable for never-ending pipelines** as
    it may overfill the memory.
    """

    _aliases_ = ['local_test',]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    async def _run(self, context, event) -> list:
        self.results = []
        pipeline = context.pipelines['main']
        runner = PipelineRunner(pipeline)
        async with context.kvstore:
            async for _event in runner(context, event):
                self.results.append(_event)
    
    def target(self, context, event) -> list:
        super().target(context, event)
        return self.results


class REPLLocalDispatcher(LocalDispatcher):
    """Runs pipelines in a single thread.

    This dispatcher append an 'output' command to the pipeline if
    necessary.
    """

    _aliases_ = ['local_repl',]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output_cmd = m42pl.command('output')
    
    def target(self, context, event):
        pipeline = context.pipelines['main']
        # Add a trailing output command if necessary
        if not len(pipeline.commands) or not isinstance(pipeline.commands[-1], self.output_cmd):
            pipeline.commands.append(self.output_cmd())
            pipeline.build()
        # Continue
        return super().target(context, event)


class DetachedLocalDispatcher(LocalDispatcher):
    """Runs pipelines in a single, detached process.

    This dispather works as a ``LocalDispatcher`` but runs in
    background in detached mode: the calling process (e.g. M42PL main)
    will return before the pipeline has ran.
    """
    
    _aliases_ = ['local_detached',]

    def _detached_target(self, *args):
        context = dill.loads(args[0])
        event = dill.loads(args[1])
        # if os.fork() != 0:
        #     return
        # Patch dynamically imported modules
        setattr(m42pl.encoders, 'ALIASES', dill.loads(args[2]))
        super().target(context, event)

    def target(self, context, event) -> int:
        """Runs the pipeline in a new process.

        :returns:   New process PID
        """
        detached = multiprocessing.Process(
            target=self._detached_target,
            args=(
                dill.dumps(context),
                dill.dumps(event),
                dill.dumps(encoders_aliases)
            )
        )
        detached.daemon = True
        detached.start()
        # detached.join()
        # print(detached)
        # print(dir(detached))
        return detached.pid

    async def status(self, identifier: int|str) -> Dispatcher.State:
        try:
            status = psutil.Process(int(identifier)).status()
            return {
                psutil.STATUS_RUNNING: self.State.RUNNING,
            }.get(status, self.State.UNKNOWN)
        except Exception:
            pass
        return self.State.UNKNOWN

    async def status_str(self, identifier: int|str) -> Dispatcher.State:
        return (await self.status(identifier)).name
