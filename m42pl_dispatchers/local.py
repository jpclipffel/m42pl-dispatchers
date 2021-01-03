import asyncio

from m42pl.dispatchers import Dispatcher
from m42pl.utils.time import now
import m42pl


class LocalDispatcher(Dispatcher):
    """Run pipeline locally.
    """
    _aliases_ = ['local',]

    def __init__(self, context: 'Context') -> None:
        super().__init__(context)

    async def _run(self, pipeline) -> None:
        async for _ in pipeline():
            pass

    def __call__(self):
        _, pipeline = list(self.context.pipelines.items())[-1]
        asyncio.run(self._run(pipeline))


class TestLocalDispatcher(LocalDispatcher):
    '''A dispatcher to be used for local tests.

    This dispatcher stores the pipeline results in a list and
    returns this list atfer the pipeline execution. This is not
    suitable for never-ending pipelines, which will of course
    saturate the memory.
    '''
    _aliases_ = ['local_test',]

    def __init__(self, context: 'Context'):
        super().__init__(context)
        self.results = []

    async def _run(self, pipeline) -> list:
        self.results = []
        async for event in pipeline():
            self.results.append(event)
        return self.results
    
    def __call__(self):
        super().__call__()
        return self.results


class ShellLocalDisptcher(LocalDispatcher):
    """A dispatcher to be used for local *shell*.

    This dispatcher append an '| output' command to the pipeline if
    necessary.
    """
    _aliases_ = ['local_shell',]

    def __init__(self, context: 'Context'):
        super().__init__(context)
    
    async def _run(self, pipeline) -> None:
        output_cmd = m42pl.command('output')
        if not len(pipeline.commands) or not isinstance(pipeline.commands[-1], output_cmd):
            pipeline.commands.append(output_cmd())
            pipeline.build()
        async for _ in pipeline():
            pass
