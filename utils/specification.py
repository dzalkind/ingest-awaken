import os

from typing import AnyStr
from .pipeline import A2ePipeline


# TODO: go for this idea – users will define a Spec class for each regex they provide
# it should verify that the paths are right and then should expand the paths to be absolute
# if needed. Then other parts of the scripts here should expect and use Spec classes. May want
# to rename this class


class IngestSpec:
    """----------------------------------------------------------------------------
    Class to group the object and specifications needed to create an A2ePipeline
    instance that will be used to ingest data.

    ----------------------------------------------------------------------------"""

    def __init__(
        self,
        pipeline: A2ePipeline,
        pipeline_config: str,
        storage_config: str,
        name: str,
    ) -> None:
        """----------------------------------------------------------------------------
        Instantiates an IngestSpec class.

        Args:
            pipeline (A2ePipeline): A child class derived from the `A2ePipeline` parent
            class.
            pipeline_config (str): The path to the pipeline config file.
            storage_config (str): The path to the storage config file.
            name (str): The name of the ingest. This should be the name of the folder
            under which the ingest resides, though it will only be used for labelling
            purposes.

        ----------------------------------------------------------------------------"""
        assert issubclass(pipeline, A2ePipeline)
        assert os.path.isfile(pipeline_config)
        assert os.path.isfile(storage_config)

        self.pipeline = pipeline
        self.pipeline_config = pipeline_config
        self.storage_config = storage_config
        self.name = name

    def instantiate(self) -> A2ePipeline:
        """----------------------------------------------------------------------------
        Instantiates the pipeline using the previously-provided specifications.

        Returns:
            A2ePipeline: An instance of the provided pipeline class.

        ----------------------------------------------------------------------------"""
        return self.pipeline.__init__(self.pipeline_config, self.storage_config)
