from typing import Dict
from utils.aws import configure_env, parse_event
from utils.dispatcher import PipelineDispatcher
from utils.logger import logger

# TODO: Examine logging more closely –  can this be improved?


def run_pipeline(event: Dict, context: object):
    """----------------------------------------------------------------------------
    Main entry point for the lambda function in AWS. This function is triggered by
    an SNS event which is expected to contain a list of input files for which an
    ingest pipeline should be run.

    Args:
        event (Dict): [description]
        context (object): [description]

    ----------------------------------------------------------------------------"""

    configure_env()

    input_files = parse_event(event, context)

    if not input_files:
        logger.info("Pipeline status: failure")
        return

    logger.info(f"Found input files: {input_files}")

    dispatcher = PipelineDispatcher(auto_discover=True)

    logger.debug(f"Discovered ingest modules: \n{dispatcher._cache._modules}")

    success = dispatcher.dispatch(input_files)

    logger.info(f"Pipeline status: {'success' if success else 'failure'}")


if __name__ == "__main__":
    # TODO: Run all the ingests locally
    pass
