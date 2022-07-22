from __future__ import annotations

import os
import asyncio
from pathlib import Path
import multiprocessing
import dill

try:
    import uvloop
    uvloop.install()
except Exception:
    pass

from m42pl.pipeline import PipelineRunner
from m42pl.dispatchers import Dispatcher
import m42pl.errors

from m42pl.encoders import ALIASES as encoders_aliases


class LocalDispatcher(Dispatcher):
    """Runs pipelines in the current process.
    """

    _aliases_ = ['local',]

    def __init__(self, workdir: str = '.', *args, **kwargs) -> None:
        """
        :param workdir: Working directory
        :param timeout: Pipeline's timeout
        """
        super().__init__(*args, **kwargs)
        self.workdir = Path(workdir)
        if not self.workdir.is_dir():
            raise m42pl.errors.DispatcherError(
                self, 
                f'Requested workdir does not exists: workdir="{workdir}"'
            )
        self.current_kvstore = None
        self.current_identifier = None

    async def __aexit__(self, *args, **kwargs):
        await self.unregister(
            self.current_kvstore,
            self.current_identifier
        )

    async def _run(self, context, event) -> None:
        """Run the context main pipeline.
        """
        async with context.kvstore:
            async with self:
                self.current_kvstore = context.kvstore
                self.current_identifier = os.getpid()
                # Write pipeline process ID to KVStore
                await self.register(
                    self.current_kvstore,
                    self.current_identifier
                )
                # Select and run pipeline
                pipeline = context.pipelines['main']
                runner = PipelineRunner(pipeline)
                self.plan.layers[0].start()
                async for _ in runner(context, event):
                    pass
                self.plan.layers[0].stop()

    def target(self, context, event, plan: bool = False):
        # Plan
        self.plan.add_layer()
        self.plan.add_pipeline('main')
        for command in context.pipelines['main'].commands:
            self.plan.add_command(command._aliases_[0])
        # Run
        if not plan:
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
    
    def target(self, context, event, plan) -> list:
        super().target(context, event, plan)
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
    
    def target(self, context, event, plan):
        pipeline = context.pipelines['main']
        # Add a trailing output command if necessary
        if not len(pipeline.commands) or not isinstance(pipeline.commands[-1], self.output_cmd):
            pipeline.commands.append(self.output_cmd(buffer=4096))
            pipeline.build()
        # Continue
        return super().target(context, event, plan)


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

    def target(self, context, event, plan) -> int:
        """Runs the pipeline in a new process.

        :returns: New process PID
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
        return detached.pid
