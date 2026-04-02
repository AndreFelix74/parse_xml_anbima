#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Feb 10 17:34:19 2026

@author: andrefelix
"""


from configparser import ConfigParser
from pathlib import Path
from typing import Any
from collections.abc import Mapping, Sequence


def required_schema() -> dict[str, list[str]]:
    return {
        'InputPaths': [
            'data_aux_path',
            'xml_source_path',
            'mec_sac_path',
            'performance_path',
        ],
        'OutputPaths': [
            'destination_path',
            'logs',
            'log_evidence_root',
            'debug_path',
        ],
        'OutputFormats': [
            'destination_file_format',
            'debug_file_format',
            'log_evidence_file_format',
        ],
        'Debug': [
            'debug',
        ],
        'Processing': [
            'workers',
        ],
    }


def validate_config_schema(config: ConfigParser) -> None:
    schema = required_schema()
    _require_sections(config, list(schema.keys()))
    for section, keys in schema.items():
        _require_keys(config, section, keys)


def _read_ini(config_file: str | Path) -> ConfigParser:
    cfg_path = Path(config_file)
    if not cfg_path.exists():
        raise FileNotFoundError(f"Config file not found: {cfg_path}")

    config = ConfigParser()
    config.read(cfg_path)
    return config


def _require_sections(config: ConfigParser, sections: list[str]) -> None:
    missing = [s for s in sections if not config.has_section(s)]
    if missing:
        raise KeyError(f"Missing section(s) in config.ini: {', '.join(missing)}")


def _require_keys(config: ConfigParser, section: str, keys: list[str]) -> None:
    missing = [k for k in keys if k not in config[section]]
    if missing:
        raise KeyError(f"Missing key(s) in [{section}]: {', '.join(missing)}")


def _project_data_root(config_file: Path) -> Path:
    """
    Replicates your previous behavior: if a path in ini is relative and does not
    start with '.', it was treated as '../data/<value>' relative to the config file.
    Here we interpret '../data' relative to the config file directory.
    """
    return (config_file.parent / '..' / 'data').resolve()


def _resolve_path(value: str, *, config_file: Path, want_dir: bool) -> Path:
    """
    - If value starts with '.' -> resolve relative to config file directory.
    - If value is absolute -> use it.
    - Otherwise -> resolve relative to '../data' (relative to config file directory),
      matching your legacy behavior.
    Returns a normalized string (no forced trailing slash; pathlib handles it).
    """
    raw = (value or '').strip()
    if not raw:
        return ''

    p = Path(raw)

    if p.is_absolute():
        resolved = p
    elif raw.startswith('.'):
        resolved = (config_file.parent / p).resolve()
    else:
        resolved = (_project_data_root(config_file) / p).resolve()

    if want_dir:
        # Ensure "directory semantics" without forcing a trailing slash.
        # If it's a file path accidentally, this will use its parent.
        resolved = resolved if resolved.suffix == '' else resolved.parent

    return resolved


def _yesno(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {'yes', 'y', 'true', '1', 'on'}


def _parse_workers(value: str | None) -> int | None:
    if value is None:
        return None
    raw = value.strip().lower()
    if raw in {'auto', ''}:
        return None
    n = int(raw)
    if n < 1:
        raise ValueError("Processing.workers must be >= 1 or 'auto'")
    return n


def load_settings(config_file: str | Path = 'config.ini') -> dict[str, Any]:
    """
    Unified config loader for Sofia.

    Returns:
        dict with keys:
          - paths: dict
          - debug: dict
          - log: dict

    Notes:
      * Validates required sections/keys.
      * Normalizes/ resolves paths using pathlib.
      * Returns a dict (not positional list).
    """
    cfg_path = Path(config_file)
    config = _read_ini(cfg_path)

    validate_config_schema(config)

    paths = {
        'data_aux_path': _resolve_path(config['InputPaths']['data_aux_path'], config_file=cfg_path, want_dir=True),
        'xml_source_path': _resolve_path(config['InputPaths']['xml_source_path'], config_file=cfg_path, want_dir=True),
        'mec_sac_path': _resolve_path(config['InputPaths']['mec_sac_path'], config_file=cfg_path, want_dir=True),
        'performance_path': _resolve_path(config['InputPaths']['performance_path'], config_file=cfg_path, want_dir=True),
        'custodia_path': _resolve_path(config['InputPaths']['custodia_path'], config_file=cfg_path, want_dir=True),
        'destination_path': _resolve_path(config['OutputPaths']['destination_path'], config_file=cfg_path, want_dir=True),
        'destination_file_format': config['OutputFormats']['destination_file_format'].strip(),
    }

    debug = {
        'save': _yesno(config['Debug'].get('debug'), default=False),
        'output_path': _resolve_path(config['OutputPaths']['debug_path'], config_file=cfg_path, want_dir=True),
        'file_format': config['OutputFormats']['debug_file_format'].strip(),
    }

    processing = {
        'workers': _parse_workers(config['Processing'].get('workers')),
    }

    log_cfg = {
        'logs': _resolve_path(config['OutputPaths']['logs'], config_file=cfg_path, want_dir=True),
        'evidence_root': _resolve_path(config['OutputPaths']['log_evidence_root'], config_file=cfg_path, want_dir=True),
        'evidence_file_format': config['OutputFormats']['log_evidence_file_format'],
    }

    return {
        'paths': paths,
        'debug': debug,
        'processing': processing,
        'log': log_cfg,
    }
