"""Logging configuration for pilot-intel."""

import logging
import platform
import sys
from datetime import datetime

import config

# ---------------------------------------------------------------------------
# Debug filter — passes pilot-intel DEBUG+ and external INFO+
# ---------------------------------------------------------------------------

_PILOT_INTEL_PREFIXES = (
    "logging_config", "agent.", "retrieval.", "ingest.", "cli", "cache.",
)


class _DebugFilter(logging.Filter):
    """Applied to stream handler in --debug mode: shows pilot-intel DEBUG+, external INFO+."""
    def filter(self, record: logging.LogRecord) -> bool:
        if any(record.name.startswith(p) for p in _PILOT_INTEL_PREFIXES):
            return True
        return record.levelno >= logging.INFO


# ---------------------------------------------------------------------------
# Per-node structured logging helpers
# ---------------------------------------------------------------------------

_node_logger = logging.getLogger("logging_config.nodes")


def log_node_input(node_name: str, state: dict) -> None:
    _node_logger.debug(
        "[%s] INPUT: question=%r | type=%s | iterations=%d",
        node_name,
        state.get("question", ""),
        state.get("question_type", ""),
        state.get("iterations", 0),
    )


def log_node_output(node_name: str, output: dict) -> None:
    _node_logger.debug(
        "[%s] OUTPUT: %s",
        node_name,
        {k: str(v)[:200] if isinstance(v, str) else v for k, v in output.items()},
    )


# ---------------------------------------------------------------------------
# Main setup
# ---------------------------------------------------------------------------

def setup_logging(command: str = "general", debug: bool = False) -> None:
    config.LOG_DIR.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = config.LOG_DIR / f"{command}_{timestamp}.log"

    fmt = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
    formatter = logging.Formatter(fmt)

    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)

    stream_handler = logging.StreamHandler(sys.stdout)
    if debug:
        stream_handler.setLevel(logging.DEBUG)
        stream_handler.addFilter(_DebugFilter())
    else:
        stream_handler.setLevel(logging.INFO)
    stream_handler.setFormatter(formatter)

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    root.addHandler(file_handler)
    root.addHandler(stream_handler)

    logger = logging.getLogger(__name__)

    try:
        import torch
        cuda_available = torch.cuda.is_available()
        gpu_name = torch.cuda.get_device_name(0) if cuda_available else "N/A"
    except Exception:
        cuda_available = False
        gpu_name = "N/A"

    logger.info("pilot-intel | command=%s | log=%s", command, log_file)
    logger.info("Python %s | %s", sys.version.split()[0], platform.platform())
    logger.info("CUDA available: %s | GPU: %s", cuda_available, gpu_name)
    logger.info("APPLYPILOT_DB: %s", config.APPLYPILOT_DB)
    logger.info("PILOT_INTEL_DIR: %s", config.PILOT_INTEL_DIR)

    if config.LANGSMITH_API_KEY:
        logger.info("LangSmith tracing enabled | project: %s", config.LANGSMITH_PROJECT)
    else:
        logger.info("LangSmith tracing disabled — set LANGSMITH_API_KEY to enable")
