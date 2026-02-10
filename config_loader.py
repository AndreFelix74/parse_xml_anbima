#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Feb 10 17:34:19 2026

@author: andrefelix
"""


from configparser import ConfigParser
from pathlib import Path
from typing import Any


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


def load_settings(config_file: str | Path = "config.ini") -> dict[str, Any]:
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

    _require_sections(config, ["Paths", "Debug"])

    # Declare the superset of keys you rely on (adjust as needed).
    _require_keys(
        config,
        'Paths',
        [
            'data_aux_path',
            'mec_sac_path',
            # Orchestration pipeline
            'xml_source_path',
            'destination_path',
            'destination_file_format',
            # Logging evidence
            'log_evidence_root',
            'log_evidence_file_format',
        ],
    )

    paths = {
        'xml_source_path': _resolve_path(config['Paths']['xml_source_path'], config_file=cfg_path, want_dir=True),
        'destination_path': _resolve_path(config['Paths']['destination_path'], config_file=cfg_path, want_dir=True),
        'destination_file_format': config['Paths']['destination_file_format'].strip(),
        'data_aux_path': _resolve_path(config['Paths']['data_aux_path'], config_file=cfg_path, want_dir=True),
        'mec_sac_path': _resolve_path(config['Paths']['mec_sac_path'], config_file=cfg_path, want_dir=True),
        'log_evidence_root': _resolve_path(config['Paths']['log_evidence_root'], config_file=cfg_path, want_dir=True),
        'log_evidence_file_format': config['Paths']['log_evidence_file_format'].strip(),
    }

    debug = {
        'save': _yesno(config['Debug'].get('debug'), default=False),
        'output_path': _resolve_path(config['Debug'].get('debug_path', ''), config_file=cfg_path, want_dir=True),
        'file_format': (config['Debug'].get('debug_file_format') or '').strip(),
    }

    log_cfg = {
        'log_evidence_root': paths['log_evidence_root'],
        'log_evidence_file_format': paths['log_evidence_file_format'],
    }

    return {
        'paths': paths,
        'debug': debug,
        'log': log_cfg,
    }
