from __future__ import annotations

import logging
import logging.config
from typing import Dict, Any


def setup_logging(logging_config: Dict[str, Any]) -> None:
    """
    Configure logging from a dict (loaded from logging.yaml).
    """
    logging.config.dictConfig(logging_config)

    # Emit a startup line so we know logging is live
    logger = logging.getLogger(__name__)
    logger.info("Logging configured")