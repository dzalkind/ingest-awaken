# Lidar Halo XRP NWTC

Ingest for the XRP Halo Lidar at the NWTC site

Writen by [Daniel Zalkind](mailto:dzalkind@nrel.gov) and [Maxwell Levin](mailto:maxwell.levin@pnnl.gov)

## Ingest Organization

The ingest takes the following layout:

- `runner.py` –– development entry point to run this pipeline on a set of input files.
- `mapping.py` –– Defines a mapping of filepath pattern to ingest specification so that
higher level processes know which ingest and configurations to use given a set of input
files.
- `__init__.py` –– Declares this folder as a Python module and provides a number of
methods / classes that upstream code can import.
upstream code can easily launch this ingest on data with the right parameters.
- `config/` –– Contains `yaml` configuration files for specifying metadata, quality
checks and handling, variable retrieval specifications, and input/output types.
- `pipeline/` –– Contains Python code meant to be modified to implement plotting
capabilities, add derived variables, implement custom `FileHandler` or qc objects, or
support any other custom functionality needed.
- `tests/` –– Contains tests that should be applied to this ingest in order to ensure
it functions as intended. Tests in here will be run any time a change is made to the
`a2edap/ingest-awaken` master branch, so the test(s) should be lightweight if possible.


## Getting started

1. Ensure that your development environment is set up according to 
[a2edap/ingest-awaken](https://github.com/a2edap/ingest-awaken).

2. Use the `TODO-Tree` VS Code extension or use the search tool to find occurances of
"`# TODO – Developer:`". Each instance of this requires your attention. Attend to all
of these TODOs and remove the "`TODO – Developer:`" text as you do so.

3. As you are developing, try to follow best practices to save yourself (and others)
time:
    - Commit your changes early, and make commits after each significant change.
    - Tests should be written as soon as possible, and you should test your code often.
    - Try to write clean and readable code that can be reused by others.
    - Document complexities such that future you and readers of this code understand
    what it is that you are doing.

4. You can run your code locally in VS Code by running the tests, or by 
running/debugging the `runner.py` script at the same level as this `README.md` file.

5. When you have finished the ingest script your tests should pass, the code should be
formatted by `black`, there should be no `flake8` warnings (Use "`# noqa`" to disable
a specific line, if need be), and this `README.md` file should be totally re-written to
describe your ingest pipeline to end-users, project maintainers, and curious onlookers
who may not be familiar with your work. If this has all been completed, then sync your
local changes with your remote fork of `a2edap/ingest-awaken` and submit a pull request
with the `master` branch so that your changes can be reviewed and merged into
production.   
