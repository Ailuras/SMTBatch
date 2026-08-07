"""Serve the reduction experiment launcher, monitor, and report pages."""

from __future__ import annotations

from .reduction_serve import (
    ReductionManager,
    controller_paths,
    dashboard_path,
    handler_factory,
    main,
    report_path,
)

__all__ = [
    "ReductionManager", "controller_paths", "dashboard_path", "handler_factory",
    "main", "report_path",
]


if __name__ == "__main__":
    raise SystemExit(main())
