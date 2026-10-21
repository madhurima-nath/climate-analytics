# src/transforms/__init__.py
# Expose transform modules.
#
# NOTE: deliberately no imports. SDP (Spark Declarative Pipelines) executes every
# file matched by the pipeline's glob libraries standalone, and relative imports
# such as `from . import energy` fail in that context (no known parent package).
# This file must therefore be safe to execute on its own.
#
# `from src.transforms import <module>` still works everywhere (tests,
# orchestrators, SDP sources): Python resolves names not bound here by importing
# the submodule directly.

__all__ = ['energy', 'weather', 'nature', 'quality', 'common', 'climate']