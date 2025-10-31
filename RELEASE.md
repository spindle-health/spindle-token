# Release Guide

This is a guide for how to publish a new release of `spindle-token` including all steps required to update documentation.

1. Checkout the `main` branch and `git pull`.

1. Ensure tests are passing.

    ```
    poetry run pytest
    ```

1. Check the versions in `pyproject.toml` and the root `__init__.py` to make sure they are both the intended version.

1. Build the library wheel. Upload to databricks and test the functionality in `docs/guides/databricks.ipynb`.

    ```
    poetry build
    ```

    If any changes are needed to the Databricks guide notebook, author them in Databricks, export as IPython notebook, and replace the guide in the repo.

1. Build the documentation site (with API docs) and check for build errors. Explore the doc site locally for any issues, including code changes needed in the getting started guide.

    ```
    poetry run mkdocs build
    poetry run mkdocs serve
    ```

1. Publish the package to PyPI.

    ```
    poetry publish
    ```

1. Publish the documentation site.

    ```
    poetry run mkdocs gh-deploy
    ```

1. Create a new release on Github.
