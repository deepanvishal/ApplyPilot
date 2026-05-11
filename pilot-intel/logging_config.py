"""Logging configuration for pilot-intel."""

import logging
import platform
import sys
from datetime import datetime

import config


def setup_logging(command: str = "general") -> None:
    config.LOG_DIR.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = config.LOG_DIR / f"{command}_{timestamp}.log"

    fmt = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
    formatter = logging.Formatter(fmt)

    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)

    stream_handler = logging.StreamHandler(sys.stdout)
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
