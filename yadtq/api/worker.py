import threading
from typing import Dict, Callable
import time
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

class TaskWorker:
    """Worker API for processing tasks from a queue"""
    def __init__(self, worker_id: str, task_handlers: Dict[str, Callable], broker, result_store):
        self.worker_id = worker_id
        self.task_handlers = task_handlers
        self._broker = broker
        self._result_store = result_store
        self._consumer = self._broker.get_consumer('yadtq_worker_group')  # Shared consumer group
        self._running = False
        self._heartbeat_thread = None

    def _send_heartbeat(self):
        """Periodically send a heartbeat signal to indicate the worker is alive."""
        while self._running:
            try:
                self._result_store.update_worker_heartbeat(self.worker_id)
                logger.debug(f"{self.worker_id} sent a heartbeat.")
            except Exception as e:
                logger.error(f"{self.worker_id} failed to send heartbeat: {e}")
            time.sleep(1)

    def _process_task(self, task_data):
        """Process a single task."""
        task_id = task_data.get('task_id')
        task_name = task_data.get('task_name')
        args = task_data.get('args', [])
        kwargs = task_data.get('kwargs', {})

        logger.info(f"{self.worker_id} received task {task_id}: {task_name} with args {args}, kwargs {kwargs}")
        
        # Update task status to 'processing'
        self._result_store.set_task_status(task_id, 'processing', worker_id=self.worker_id)

        try:
            # Call the appropriate handler
            handler = self.task_handlers.get(task_name)
            if not handler:
                raise ValueError(f"No handler found for task: {task_name}")
            
            result = handler(*args, **kwargs)
            
            # Update task status to 'success'
            self._result_store.set_task_status(task_id, 'success', result=result)
            logger.info(f"{self.worker_id} successfully completed task {task_id}: {result}")
        
        except Exception as e:
            # Update task status to 'failed'
            error_message = str(e)
            self._result_store.set_task_status(task_id, 'failed', error=error_message)
            logger.error(f"{self.worker_id} failed task {task_id}: {error_message}")

    def _poll_tasks(self):
        """Poll for tasks and process them."""
        while self._running:
            try:
                messages = self._consumer.poll(timeout_ms=1000)
                if not messages:
                    logger.debug(f"{self.worker_id} found no new messages.")
                    continue
                
                for topic_partition, batch in messages.items():
                    for message in batch:
                        task_data = message.value
                        self._process_task(task_data)  # Process each task
                        self._consumer.commit()  # Commit offset after successful processing
            
            except Exception as e:
                logger.error(f"{self.worker_id} encountered an error while polling tasks: {e}")

    def start(self):
        """Start the worker and begin processing tasks."""
        self._running = True

        # Start heartbeat thread
        self._heartbeat_thread = threading.Thread(target=self._send_heartbeat, daemon=True)
        self._heartbeat_thread.start()

        logger.info(f"{self.worker_id} has started and is ready to process tasks.")
        try:
            self._poll_tasks()  # Start polling for tasks
        except KeyboardInterrupt:
            logger.warning(f"{self.worker_id} received shutdown signal.")
        finally:
            self.stop()

    def stop(self):
        """Stop the worker gracefully."""
        self._running = False
        if self._heartbeat_thread and self._heartbeat_thread.is_alive():
            self._heartbeat_thread.join(timeout=1)
        logger.info(f"{self.worker_id} has stopped.")

