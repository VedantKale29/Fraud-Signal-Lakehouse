"""Shared fixtures. LOG_TO_FILE=0 keeps CI containers clean."""

import os

os.environ.setdefault("LOG_TO_FILE", "0")
