#!/usr/bin/env python3
"""Entry point to run the tick stream ingestor."""

import asyncio
from src.ingestor import main

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        # Graceful exit on Ctrl+C
        pass
