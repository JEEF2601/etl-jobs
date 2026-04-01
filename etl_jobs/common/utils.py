from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml


def get_env(name: str, default: str | None = None, required: bool = True) -> str:
    """Lee una variable de entorno; lanza ValueError si es requerida y no está definida."""
    value = os.getenv(name, default)
    if required and (value is None or value.strip() == ""):
        raise ValueError(f"Missing required environment variable: {name}")
    return value  # type: ignore[return-value]


def load_yaml(path: str | Path) -> dict[str, Any]:
    with Path(path).open("r", encoding="utf-8") as file:
        return yaml.safe_load(file) or {}
