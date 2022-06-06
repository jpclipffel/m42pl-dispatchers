import asyncio
from mpi4py import MPI

import m42pl
from m42pl.context import Context
from m42pl.pipeline import Pipeline, PipelineRunner
from m42pl.dispatchers import Dispatcher, Plan
from m42pl.commands import MergingCommand


COMM = MPI.COMM_WORLD
RANK = COMM.Get_rank()
SIZE = COMM.Get_size()


class MPI(Dispatcher):
    """Run pipelines in pre-allocated MPI slots.
    """

    _aliases_ = ['mpi', ]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def target(self, context, event, plan):

        async def run(pipeline, context, event):
            print(f'Rank {RANK}: {pipeline.commands}')
            async with context.kvstore:
                runner = PipelineRunner(pipeline)
                async for _ in runner(context, event):
                    pass
            print(f'Rank {RANK}: Done')

        pipelines = self.split_pipeline(context.pipelines['main'])
        main_pipeline = None

        # ---
        # Specialize pipelines
        for layer, pipeline in enumerate(pipelines):
            # Pre-merging pipeline
            # Append mpi-send
            if layer == 0 and RANK > 0:
                pipeline.commands.append(
                    m42pl.command('mpi-send')(COMM)
                )
                pipeline.build()
                pipeline.set_chunk(RANK, SIZE-1)
                context.pipelines['main'] = pipeline
            # Post-merging pipeline
            elif layer > 0 and RANK == 0:
                pipeline.commands = [
                    m42pl.command('mpi-receive')(COMM, SIZE-1)
                ] + pipeline.commands
                pipeline.build()
                pipeline.set_chunk(1, 1)
                context.pipelines['main'] = pipeline
            # Ignore
            # else:
            #     context.pipelines['main'] = None
        # ---
        # Run pipelines
        # if context.pipelines['main'] is not None:
        #     print(f'Rank {RANK}: {context.pipelines["main"].commands}')
        if context.pipelines['main'] is not None:
            asyncio.run(run(context.pipelines['main'], context, event))
