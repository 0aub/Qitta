from __future__ import annotations

import asyncio
import datetime
import logging
import os
from typing import Any, Awaitable, Callable, Dict, List, Optional

from playwright.async_api import Browser

from .jobs import JobStore


TaskFunc = Callable[..., Awaitable[Dict[str, Any]]]


class Worker:
    """Processes jobs from the queue using a shared Browser instance."""

    def __init__(
        self,
        index: int,
        *,
        store: JobStore,
        shared_browser: Browser,
        task_registry: Dict[str, TaskFunc],
        data_root: str,
        base_logger: logging.Logger,
    ) -> None:
        self.index = index
        self.store = store
        self.browser = shared_browser
        self.task_registry = task_registry
        self.data_root = data_root
        self.base_logger = base_logger
        self._task: Optional[asyncio.Task] = None
        self._logger = logging.getLogger(f"browser.worker-{index}")
        if not self._logger.handlers:
            for h in self.base_logger.handlers:
                self._logger.addHandler(h)
            self._logger.setLevel(logging.INFO)

    def start(self) -> asyncio.Task:
        self._task = asyncio.create_task(self._run_loop(), name=f"worker-{self.index}")
        return self._task

    async def cancel(self) -> None:
        if self._task:
            self._task.cancel()
            with contextlib.suppress(Exception):
                await self._task

    async def _run_loop(self) -> None:
        # Ensure the shared browser exists
        while not self.browser:
            await asyncio.sleep(0.1)

        while True:
            job_id = await self.store.queue.get()
            job = self.store.jobs[job_id]
            job.started_at = datetime.datetime.utcnow()
            job.status = "running"

            # Output directory per job
            job_output_dir = os.path.join(self.data_root, job.task_name, job_id)
            os.makedirs(job_output_dir, exist_ok=True)

            # Per-job logger
            job_log_path = os.path.join(job_output_dir, "job.log")
            job_logger = logging.getLogger(f"browser.job.{job_id}")
            if not job_logger.handlers:
                fh = logging.FileHandler(job_log_path)
                fh.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s"))
                job_logger.addHandler(fh)
                for h in self.base_logger.handlers:
                    job_logger.addHandler(h)
                job_logger.setLevel(logging.INFO)

            try:
                # Per-job headless override: launch a separate browser if needed
                headless_override = job.params.pop("_headless_override", None)
                if headless_override is not None:
                    job_browser = await self.browser.browser_type.launch(headless=headless_override)
                else:
                    job_browser = self.browser

                # Execute the task
                task_fn = self.task_registry[job.task_name]
                result = await task_fn(
                    browser=job_browser,
                    params=job.params,
                    job_output_dir=job_output_dir,
                    logger=job_logger,
                )
                job.result = result
                job.status = "finished"
            except Exception as exc:
                job.error = str(exc)
                job.status = "error"
                job_logger.error(f"Job {job_id} failed: {exc}")
            finally:
                job.finished_at = datetime.datetime.utcnow()
                self.store.queue.task_done()
                if "job_browser" in locals() and job_browser is not self.browser:
                    try:
                        await job_browser.close()
                    except Exception:
                        pass


class WorkerPool:
    """Spawns and manages a group of Workers."""

    def __init__(
        self,
        *,
        store: JobStore,
        shared_browser: Browser,
        task_registry: Dict[str, TaskFunc],
        data_root: str,
        base_logger: logging.Logger,
    ) -> None:
        self.store = store
        self.shared_browser = shared_browser
        self.task_registry = task_registry
        self.data_root = data_root
        self.base_logger = base_logger
        self._workers: List[Worker] = []
        self._tasks: List[asyncio.Task] = []

    def start(self, n: int) -> None:
        for i in range(n):
            w = Worker(
                i,
                store=self.store,
                shared_browser=self.shared_browser,
                task_registry=self.task_registry,
                data_root=self.data_root,
                base_logger=self.base_logger,
            )
            self._workers.append(w)
            self._tasks.append(w.start())

    async def stop(self) -> None:
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
