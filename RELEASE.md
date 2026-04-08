# Release Guide

This is a guide for how to publish a new release of `spindle-token` including all steps required to update documentation.

1. Checkout the `main` branch and `git pull`.

1. Set Poetry to use Python 3.12.

    ```
    poetry env use python3.12
    ```

1. Ensure tests are passing.

    ```
    poetry run pytest
    ```

1. Check code formatting.

    ```
    poetry run black --check .
    ```

1. Check the versions in `pyproject.toml` and the root `__init__.py` to make sure they are both the intended version.

1. Build the library wheel locally.

    ```
    poetry build
    ```

    This creates the release artifacts in `dist/` on your local machine.

1. Use the built wheel to validate the Databricks notebook flow.

    Install the wheel into the target Databricks environment and run `docs/guides/databricks.ipynb` to confirm the notebook still works end to end.

    If any changes are needed to the Databricks guide notebook, author them in Databricks, export as IPython notebook, and replace the guide in the repo.

1. Build the documentation site (with API docs) and check for build errors. Explore the doc site locally for any issues, including code changes needed in the getting started guide.

    ```
    poetry run mkdocs build
    poetry run mkdocs serve
    ```

1. Create a draft release on Github for the current tag and add the release notes.

    1. Confirm the release tag matches the version you are shipping, for example `v2.0.0`.
    2. Open the GitHub releases page and start a new release from that tag.
    3. Set the release title to the same version string, for example `v2.0.0`.
    4. Paste the release notes from `CHANGELOG.md`.
    5. Save the release as a draft so it is ready to publish after the package and docs are live.

1. Publish the package to PyPI.

    ```
    poetry publish
    ```

1. Publish the documentation site.

    ```
    poetry run mkdocs gh-deploy
    ```

1. Finalize the GitHub release after PyPI publish succeeds and the docs site is live.

    1. Confirm the PyPI publish completed successfully.
    2. Confirm the docs site deployment completed successfully.
    3. Open the draft GitHub release created earlier.
    4. Review the tag, title, and release notes one last time.
    5. Publish the release so the tag and notes become public.
