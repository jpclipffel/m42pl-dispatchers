import os
import asyncio
from pathlib import Path

from m42pl.dispatchers import Dispatcher
import m42pl.errors


class LocalDispatcher(Dispatcher):
    """Runs pipelines in the current process.
    """

    _aliases_ = ['local',]

    def __init__(self, workdir: str = '.', uv: bool = False,
                    *args, **kwargs) -> None:
        """
        :param workdir:     Working directory
        :param uv:          Use uvloop or not; Default to `False`
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

    async def _run(self, context, event) -> None:
        """Run the context main pipeline.
        """
        async with context.kvstore:
            pipeline = context.pipelines['main']
            async for _ in pipeline(context, event):
                pass

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
        async with context.kvstore:
            async for _event in pipeline(context, event):
                self.results.append(_event)
    
    def target(self, context, event) -> list:
        super().target(context, event)
        return self.results


class REPLLocalDisptcher(LocalDispatcher):
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
