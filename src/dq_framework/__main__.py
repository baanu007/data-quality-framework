"""Allow `python -m dq_framework ...` to dispatch to the runner CLI."""

from dq_framework.runner import main

if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
