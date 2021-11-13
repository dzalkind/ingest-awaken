# A2E-AWAKEN Data Ingestion Pipelines

[![tests](https://github.com/a2edap/ingest-awaken/actions/workflows/tests.yml/badge.svg)](https://github.com/a2edap/ingest-awaken/actions/workflows/tests.yml)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

This repository contains data ingestion pipelines developed for the Atmosphere to
Electrons (A2e) [American Wake Experiment (AWAKEN)](https://a2e.energy.gov/projects/awaken)
project.

## How it works

- **`runner.py`**: main entry script.
- **`ingest/<*_ingest>`**: collection of python modules, each of which is a
self-describing and self-contained ingest. Every ingest module exports the necessary
information for the `runner` to instantiate and run the ingest.
- **`utils/cache.py`**: discovery and registration of ingests.
- **`utils/dispatcher.py`**: selects, instantiates, and runs the appropriate cached
ingest.
- **`utils/env.py`**: provides utility methods for setting environment variables used
in development and production modes.
- **`utils/logger.py`**: provides structured logging mechanisms so logs can be more
easily in AWS.
- **`utils/pipeline.py`**: provides custom `A2ePipeline` class used by ingests.
- **`utils/specification.py`**: declares the `IngestSpec` class, used to group the
parameters needed to instantiate an `A2ePipeline` class.
- **`utils/utils.py`**: other miscellaneous utility methods.
- **`tests/test_ingests.py`**: sanity checks on all ingests.



## Adding a new pipeline

Developers should follow the following five-step process to create a new ingest
pipeline.

1. Fork this repository to your own github account.
2. Set up your development environment (shown below)
3. Use [awaken-cookiecutter](https://github.com/a2edap/awaken-cookiecutter) to generate
a template ingest:
    ```bash
    cookiecutter https://github.com/a2edap/awaken-cookiecutter -o ingest/
    ```
4. Modify the template as directed using best practices (code style, testing, docs).
5. Submit a pull request describing what you did.

Repository maintainers will then review the ingest submitted and work with developers
to make any changes, if needed, before accepting the pull request and deploying the
ingest to our production environment.


## Development Environment Setup

This section outlines how to set up the recommended development environment for this
project. Of course, developers are free to use their own development environment, but
they risk of experiencing delays in their pull request being accepted due to code that
does not pass tests, meet code style, or has other errors. Unifying the development
environments used by developers on this project also allows us to provide better
support to developers if they run into other problems.

The steps to set up the recommended development environment are listed below:

1. Download and install [VS Code](https://code.visualstudio.com). Make sure to add 
`code` to your path if prompted.

    We chose VS Code because of its clean user interface, quick startup time, extremely
    powerful capabilities out-of-box, and its rich library of open source extensions.

2. Clone your fork of this repository to your laptop and open it up in VS Code:
    ```bash
    git clone https://github.com/<your-username>/ingest-awaken.git
    code ingest-awaken
    ```
    *Note that the "`code ingest-awaken`" step will only work if `code` has been added
    to your path. Open the folder in VS Code manually if this is the case.*

3. The first time you open the `ingest-awaken` project in VS Code you will be prompted
to install the recommended extensions. Please do so now.

4. **Windows users**: We strongly recommend using 
[Docker](https://www.docker.com/products/docker-desktop) to manage dependencies for
this project. If you chose to use Docker, skip steps 5 & 6 and follow the steps below
instead:
    - Press `F1` (or `ctrl-shift-p`) to bring up the command pane in VS Code
    - In the command pane, type: `Remote-Containers: Open Folder in Container...` and
    hit `return`
    - You will be prompted to specify which folder should be opened. Select the folder
    containing this file (`ingest-awaken`).
    - Several dialog boxes may appear while the VS Code window is refreshing. Please
    install the recommended extensions via the dialog box. An additional dialog box
    should appear asking you to reload the window so Pylance can take effect. Please do
    this as well.
    - After the window refreshes your development environment will be set up correctly.
    You may skip steps 5. and 6.

    You can find more information about VS Code and docker containers
    [here](https://code.visualstudio.com/docs/remote/containers).

    on your machine. and connect VS Code to the [tsdat docker container](https://hub.docker.com/repository/docker/tsdat/tsdat/general).**
    We have found that some `tsdat` dependencies may not work properly on some Windows
    computers under specific conditions, causing errors that are sometimes hard to detect.
    Using the official docker container will prevent these issues. This step is optional
    for Mac and Linux users, though still encouraged.

5. We highly recommend using [conda](https://docs.anaconda.com/anaconda/install/) to
manage dependencies in your development environment. Please install this using the link
above if you haven't already done so. Then run the following commands to create your
environment:
    
    ```bash
    $ conda create --name ingest-awaken python=3.8
    $ conda activate ingest-awaken
    (ingest-awaken) $ pip install -r requirements-dev.txt
    ```

6. Tell VS Code to use your new `conda` environment:
    - Press `F1` (or `ctrl-shift-p`) to bring up the command pane in VS Code
    - In the command pane, type: `Python: Select Interpreter` and hit `return`
    - Select the newly-created `ingest-awaken` conda environment from the list. Note
    that you may need to refresh the list (cycle icon in the top right) for it to show
    up.
    - Reload the VS Code window to ensure that this setting propagates correctly.
    This is probably not needed, but doesn't hurt. To do this, press `F1` to open
    the control pane again and type `Developer: Reload Window`.
