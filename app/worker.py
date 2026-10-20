"""Durable background worker entry point for Transaction Webhook Service."""

import asyncio
import signal
import sys
from typing import Optional

from app.config import Settings, get_settings
from app.database import get_engine, session_scope
from app.logging_config import configure_logging, get_logger
from app.services.worker_service import WorkerService

logger = get_logger("worker")


class TransactionWorker:
    """Long-running worker process polling and executing background transaction jobs."""

    def __init__(self, settings: Optional[Settings] = None):
        self.settings = settings or get_settings()
        self.service = WorkerService(settings=self.settings)
        self.running = False
        self._shutdown_event = asyncio.Event()

    def stop(self) -> None:
        """Signal worker to stop gracefully."""
        logger.info("Worker stop signal received; initiating graceful shutdown...")
        self.running = False
        self._shutdown_event.set()

    async def run(self, max_iterations: Optional[int] = None) -> None:
        """Main worker execution loop."""
        self.running = True
        logger.info("=" * 60)
        logger.info("Transaction Worker Started")
        logger.info("  Poll Interval:     %.1fs", self.settings.WORKER_POLL_INTERVAL_SECONDS)
        logger.info("  Processing Delay:  %.1fs", self.settings.WORKER_PROCESSING_DELAY_SECONDS)
        logger.info("  Max Attempts:      %d", self.settings.WORKER_MAX_ATTEMPTS)
        logger.info("  Stale Lock Timeout: %.1fs", self.settings.WORKER_STALE_LOCK_SECONDS)
        logger.info("  Target Database:   %s", self.settings.sanitized_database_url)
        logger.info("=" * 60)

        iterations = 0
        while self.running:
            if max_iterations is not None and iterations >= max_iterations:
                logger.info("Reached maximum iterations (%d); stopping worker.", max_iterations)
                break

            iterations += 1

            try:
                # 1. Recover any stale locks left behind by crashed or timed-out workers
                with session_scope() as db:
                    self.service.recover_stale_jobs(db)

                # 2. Claim eligible pending jobs
                with session_scope() as db:
                    claimed_ids = self.service.claim_jobs(db)

                # 3. Process claimed jobs (transaction lock is already committed/released)
                if claimed_ids:
                    for job_id in claimed_ids:
                        if not self.running:
                            break
                        await self.service.execute_job(job_id)
                else:
                    # No work available; wait for poll interval before next cycle
                    try:
                        await asyncio.wait_for(
                            self._shutdown_event.wait(),
                            timeout=self.settings.WORKER_POLL_INTERVAL_SECONDS,
                        )
                    except asyncio.TimeoutError:
                        pass

            except asyncio.CancelledError:
                logger.info("Worker loop cancelled.")
                break
            except Exception as exc:
                logger.exception("Unexpected error in worker loop: %s", exc)
                # Avoid aggressive spinning on connection failure
                await asyncio.sleep(self.settings.WORKER_POLL_INTERVAL_SECONDS)

        logger.info("Worker loop terminated cleanly.")


def handle_signals(worker: TransactionWorker) -> None:
    """Attach OS interrupt handlers for clean shutdown across platforms."""
    def _signal_handler(sig, frame):
        logger.info("OS signal %s received.", sig)
        worker.stop()

    try:
        signal.signal(signal.SIGINT, _signal_handler)
        signal.signal(signal.SIGTERM, _signal_handler)
    except (ValueError, AttributeError):
        pass


def main() -> None:
    """Entry point for standalone execution: python -m app.worker."""
    settings = get_settings()
    configure_logging(level_name=settings.LOG_LEVEL)

    worker = TransactionWorker(settings=settings)
    handle_signals(worker)

    try:
        asyncio.run(worker.run())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Worker process exited via keyboard interrupt.")
    except Exception as exc:
        logger.critical("Fatal worker exception: %s", exc, exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
