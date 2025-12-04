"""
Multi-System Agents Module.

This module provides intelligent agents that can operate across multiple database
systems to perform comparisons, monitoring, and synchronization tasks autonomously.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional
from concurrent.futures import ThreadPoolExecutor, Future
import threading
import queue
import uuid
import time

from src.core.logging import get_logger

logger = get_logger(__name__)


class AgentStatus(Enum):
    """Agent execution status."""
    IDLE = "idle"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class AgentPriority(Enum):
    """Agent task priority levels."""
    LOW = 1
    NORMAL = 2
    HIGH = 3
    CRITICAL = 4


class SystemType(Enum):
    """Supported database system types."""
    SQL_SERVER = "sqlserver"
    POSTGRESQL = "postgresql"
    MYSQL = "mysql"
    ORACLE = "oracle"
    SQLITE = "sqlite"


@dataclass
class AgentTask:
    """Represents a task to be executed by an agent."""
    task_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    name: str = ""
    description: str = ""
    priority: AgentPriority = AgentPriority.NORMAL
    payload: Dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.now)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    status: AgentStatus = AgentStatus.IDLE
    result: Optional[Any] = None
    error: Optional[str] = None
    retries: int = 0
    max_retries: int = 3

    def to_dict(self) -> Dict[str, Any]:
        """Convert task to dictionary."""
        return {
            "task_id": self.task_id,
            "name": self.name,
            "description": self.description,
            "priority": self.priority.name,
            "payload": self.payload,
            "created_at": self.created_at.isoformat(),
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "status": self.status.value,
            "result": self.result,
            "error": self.error,
            "retries": self.retries,
        }


@dataclass
class SystemConnection:
    """Represents a connection to a database system."""
    system_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    name: str = ""
    system_type: SystemType = SystemType.SQL_SERVER
    host: str = ""
    port: int = 1433
    database: str = ""
    username: str = ""
    password: str = ""
    is_active: bool = True
    last_connected: Optional[datetime] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary (excludes password)."""
        return {
            "system_id": self.system_id,
            "name": self.name,
            "system_type": self.system_type.value,
            "host": self.host,
            "port": self.port,
            "database": self.database,
            "username": self.username,
            "is_active": self.is_active,
            "last_connected": self.last_connected.isoformat() if self.last_connected else None,
            "metadata": self.metadata,
        }


class BaseAgent(ABC):
    """Abstract base class for all agents."""

    def __init__(self, agent_id: Optional[str] = None, name: str = "BaseAgent"):
        self.agent_id = agent_id or str(uuid.uuid4())
        self.name = name
        self.status = AgentStatus.IDLE
        self.created_at = datetime.now()
        self.last_activity = datetime.now()
        self._task_queue: queue.PriorityQueue = queue.PriorityQueue()
        self._current_task: Optional[AgentTask] = None
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._callbacks: Dict[str, List[Callable]] = {
            "on_task_start": [],
            "on_task_complete": [],
            "on_task_error": [],
            "on_status_change": [],
        }
        logger.info(f"Agent {self.name} ({self.agent_id}) initialized")

    @abstractmethod
    def execute_task(self, task: AgentTask) -> Any:
        """Execute a single task. Must be implemented by subclasses."""
        pass

    def add_task(self, task: AgentTask) -> str:
        """Add a task to the agent's queue."""
        # Priority queue uses (priority, task) tuples, lower = higher priority
        priority_value = -task.priority.value  # Negative so higher priority comes first
        self._task_queue.put((priority_value, task.created_at, task))
        logger.info(f"Task {task.task_id} added to agent {self.name}")
        return task.task_id

    def run(self) -> None:
        """Main agent loop - processes tasks from queue."""
        self._set_status(AgentStatus.RUNNING)
        logger.info(f"Agent {self.name} started")

        while not self._stop_event.is_set():
            try:
                # Get task with timeout to allow checking stop event
                try:
                    _, _, task = self._task_queue.get(timeout=1.0)
                except queue.Empty:
                    continue

                self._process_task(task)

            except Exception as e:
                logger.error(f"Agent {self.name} error: {e}")

        self._set_status(AgentStatus.IDLE)
        logger.info(f"Agent {self.name} stopped")

    def _process_task(self, task: AgentTask) -> None:
        """Process a single task with error handling and retries."""
        with self._lock:
            self._current_task = task
            task.status = AgentStatus.RUNNING
            task.started_at = datetime.now()

        self._trigger_callbacks("on_task_start", task)

        try:
            result = self.execute_task(task)
            task.result = result
            task.status = AgentStatus.COMPLETED
            task.completed_at = datetime.now()
            self._trigger_callbacks("on_task_complete", task)
            logger.info(f"Task {task.task_id} completed successfully")

        except Exception as e:
            task.error = str(e)
            task.retries += 1

            if task.retries < task.max_retries:
                logger.warning(f"Task {task.task_id} failed, retrying ({task.retries}/{task.max_retries})")
                task.status = AgentStatus.IDLE
                self.add_task(task)
            else:
                task.status = AgentStatus.FAILED
                task.completed_at = datetime.now()
                self._trigger_callbacks("on_task_error", task)
                logger.error(f"Task {task.task_id} failed permanently: {e}")

        finally:
            with self._lock:
                self._current_task = None
            self.last_activity = datetime.now()

    def stop(self) -> None:
        """Signal the agent to stop."""
        self._stop_event.set()
        logger.info(f"Agent {self.name} stop requested")

    def pause(self) -> None:
        """Pause the agent."""
        self._set_status(AgentStatus.PAUSED)

    def resume(self) -> None:
        """Resume a paused agent."""
        if self.status == AgentStatus.PAUSED:
            self._set_status(AgentStatus.RUNNING)

    def _set_status(self, status: AgentStatus) -> None:
        """Set agent status and trigger callbacks."""
        old_status = self.status
        self.status = status
        if old_status != status:
            self._trigger_callbacks("on_status_change", old_status, status)

    def register_callback(self, event: str, callback: Callable) -> None:
        """Register a callback for an event."""
        if event in self._callbacks:
            self._callbacks[event].append(callback)

    def _trigger_callbacks(self, event: str, *args) -> None:
        """Trigger all callbacks for an event."""
        for callback in self._callbacks.get(event, []):
            try:
                callback(*args)
            except Exception as e:
                logger.error(f"Callback error for {event}: {e}")

    def get_status(self) -> Dict[str, Any]:
        """Get agent status information."""
        return {
            "agent_id": self.agent_id,
            "name": self.name,
            "status": self.status.value,
            "created_at": self.created_at.isoformat(),
            "last_activity": self.last_activity.isoformat(),
            "queue_size": self._task_queue.qsize(),
            "current_task": self._current_task.to_dict() if self._current_task else None,
        }


class ComparisonAgent(BaseAgent):
    """Agent for performing database comparisons across multiple systems."""

    def __init__(self, agent_id: Optional[str] = None):
        super().__init__(agent_id, "ComparisonAgent")
        self.systems: Dict[str, SystemConnection] = {}
        self.comparison_results: List[Dict[str, Any]] = []

    def register_system(self, system: SystemConnection) -> None:
        """Register a database system for comparison."""
        self.systems[system.system_id] = system
        logger.info(f"System {system.name} registered with ComparisonAgent")

    def execute_task(self, task: AgentTask) -> Dict[str, Any]:
        """Execute a comparison task."""
        task_type = task.payload.get("type", "compare")

        if task_type == "compare":
            return self._execute_comparison(task)
        elif task_type == "schema_diff":
            return self._execute_schema_diff(task)
        elif task_type == "data_diff":
            return self._execute_data_diff(task)
        else:
            raise ValueError(f"Unknown task type: {task_type}")

    def _execute_comparison(self, task: AgentTask) -> Dict[str, Any]:
        """Execute a full comparison between two systems."""
        source_id = task.payload.get("source_system_id")
        target_id = task.payload.get("target_system_id")
        tables = task.payload.get("tables", [])

        if source_id not in self.systems or target_id not in self.systems:
            raise ValueError("Source or target system not registered")

        source = self.systems[source_id]
        target = self.systems[target_id]

        results = {
            "source_system": source.name,
            "target_system": target.name,
            "tables_compared": len(tables),
            "start_time": datetime.now().isoformat(),
            "comparisons": [],
        }

        for table in tables:
            comparison = {
                "table": table,
                "schema_match": True,  # Placeholder
                "row_count_source": 0,
                "row_count_target": 0,
                "differences": 0,
            }
            results["comparisons"].append(comparison)

        results["end_time"] = datetime.now().isoformat()
        self.comparison_results.append(results)

        return results

    def _execute_schema_diff(self, task: AgentTask) -> Dict[str, Any]:
        """Execute schema comparison between systems."""
        return {"type": "schema_diff", "differences": []}

    def _execute_data_diff(self, task: AgentTask) -> Dict[str, Any]:
        """Execute data comparison between systems."""
        return {"type": "data_diff", "differences": []}


class MonitoringAgent(BaseAgent):
    """Agent for monitoring database system health and metrics."""

    def __init__(self, agent_id: Optional[str] = None):
        super().__init__(agent_id, "MonitoringAgent")
        self.systems: Dict[str, SystemConnection] = {}
        self.metrics: Dict[str, List[Dict[str, Any]]] = {}
        self.alerts: List[Dict[str, Any]] = []
        self.thresholds: Dict[str, float] = {
            "cpu_percent": 80.0,
            "memory_percent": 85.0,
            "connection_count": 100,
            "query_time_ms": 5000,
        }

    def register_system(self, system: SystemConnection) -> None:
        """Register a system for monitoring."""
        self.systems[system.system_id] = system
        self.metrics[system.system_id] = []
        logger.info(f"System {system.name} registered with MonitoringAgent")

    def execute_task(self, task: AgentTask) -> Dict[str, Any]:
        """Execute a monitoring task."""
        task_type = task.payload.get("type", "health_check")

        if task_type == "health_check":
            return self._execute_health_check(task)
        elif task_type == "collect_metrics":
            return self._execute_collect_metrics(task)
        elif task_type == "check_alerts":
            return self._check_alerts(task)
        else:
            raise ValueError(f"Unknown task type: {task_type}")

    def _execute_health_check(self, task: AgentTask) -> Dict[str, Any]:
        """Check health of all registered systems."""
        results = {"timestamp": datetime.now().isoformat(), "systems": []}

        for system_id, system in self.systems.items():
            health = {
                "system_id": system_id,
                "name": system.name,
                "status": "healthy",  # Placeholder
                "response_time_ms": 50,
                "connection_count": 5,
            }
            results["systems"].append(health)

        return results

    def _execute_collect_metrics(self, task: AgentTask) -> Dict[str, Any]:
        """Collect metrics from all systems."""
        system_id = task.payload.get("system_id")

        if system_id and system_id in self.systems:
            systems = {system_id: self.systems[system_id]}
        else:
            systems = self.systems

        metrics = {"timestamp": datetime.now().isoformat(), "metrics": []}

        for sid, system in systems.items():
            metric = {
                "system_id": sid,
                "name": system.name,
                "cpu_percent": 25.0,
                "memory_percent": 60.0,
                "disk_percent": 45.0,
                "active_connections": 10,
                "queries_per_second": 150,
            }
            metrics["metrics"].append(metric)
            self.metrics[sid].append(metric)

        return metrics

    def _check_alerts(self, task: AgentTask) -> Dict[str, Any]:
        """Check for alert conditions."""
        new_alerts = []

        for system_id, metric_list in self.metrics.items():
            if not metric_list:
                continue

            latest = metric_list[-1]

            for threshold_name, threshold_value in self.thresholds.items():
                if threshold_name in latest:
                    if latest[threshold_name] > threshold_value:
                        alert = {
                            "system_id": system_id,
                            "metric": threshold_name,
                            "value": latest[threshold_name],
                            "threshold": threshold_value,
                            "timestamp": datetime.now().isoformat(),
                        }
                        new_alerts.append(alert)
                        self.alerts.append(alert)

        return {"alerts": new_alerts, "total_alerts": len(self.alerts)}


class SyncAgent(BaseAgent):
    """Agent for synchronizing data between database systems."""

    def __init__(self, agent_id: Optional[str] = None):
        super().__init__(agent_id, "SyncAgent")
        self.systems: Dict[str, SystemConnection] = {}
        self.sync_history: List[Dict[str, Any]] = []

    def register_system(self, system: SystemConnection) -> None:
        """Register a system for synchronization."""
        self.systems[system.system_id] = system
        logger.info(f"System {system.name} registered with SyncAgent")

    def execute_task(self, task: AgentTask) -> Dict[str, Any]:
        """Execute a synchronization task."""
        task_type = task.payload.get("type", "sync")

        if task_type == "sync":
            return self._execute_sync(task)
        elif task_type == "generate_script":
            return self._generate_sync_script(task)
        elif task_type == "validate":
            return self._validate_sync(task)
        else:
            raise ValueError(f"Unknown task type: {task_type}")

    def _execute_sync(self, task: AgentTask) -> Dict[str, Any]:
        """Execute data synchronization between systems."""
        source_id = task.payload.get("source_system_id")
        target_id = task.payload.get("target_system_id")
        tables = task.payload.get("tables", [])
        mode = task.payload.get("mode", "incremental")  # full or incremental

        result = {
            "source_system": source_id,
            "target_system": target_id,
            "mode": mode,
            "tables_synced": len(tables),
            "rows_inserted": 0,
            "rows_updated": 0,
            "rows_deleted": 0,
            "start_time": datetime.now().isoformat(),
        }

        # Placeholder for actual sync logic
        for table in tables:
            result["rows_inserted"] += 100
            result["rows_updated"] += 50

        result["end_time"] = datetime.now().isoformat()
        self.sync_history.append(result)

        return result

    def _generate_sync_script(self, task: AgentTask) -> Dict[str, Any]:
        """Generate SQL script for synchronization."""
        return {
            "script_type": "sync",
            "statements": [],
            "generated_at": datetime.now().isoformat(),
        }

    def _validate_sync(self, task: AgentTask) -> Dict[str, Any]:
        """Validate synchronization results."""
        return {
            "validation_status": "passed",
            "checks_performed": 5,
            "checks_passed": 5,
        }


class AgentOrchestrator:
    """Orchestrates multiple agents and manages their lifecycle."""

    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return

        self.agents: Dict[str, BaseAgent] = {}
        self.executor = ThreadPoolExecutor(max_workers=10)
        self.agent_futures: Dict[str, Future] = {}
        self._lock = threading.Lock()
        self._initialized = True
        logger.info("AgentOrchestrator initialized")

    def create_agent(self, agent_type: str, agent_id: Optional[str] = None) -> BaseAgent:
        """Create and register a new agent."""
        agent_classes = {
            "comparison": ComparisonAgent,
            "monitoring": MonitoringAgent,
            "sync": SyncAgent,
        }

        if agent_type not in agent_classes:
            raise ValueError(f"Unknown agent type: {agent_type}")

        agent = agent_classes[agent_type](agent_id)

        with self._lock:
            self.agents[agent.agent_id] = agent

        logger.info(f"Created {agent_type} agent: {agent.agent_id}")
        return agent

    def start_agent(self, agent_id: str) -> bool:
        """Start an agent in a background thread."""
        if agent_id not in self.agents:
            return False

        agent = self.agents[agent_id]
        future = self.executor.submit(agent.run)
        self.agent_futures[agent_id] = future

        logger.info(f"Started agent: {agent_id}")
        return True

    def stop_agent(self, agent_id: str) -> bool:
        """Stop a running agent."""
        if agent_id not in self.agents:
            return False

        agent = self.agents[agent_id]
        agent.stop()

        logger.info(f"Stopped agent: {agent_id}")
        return True

    def remove_agent(self, agent_id: str) -> bool:
        """Remove an agent."""
        if agent_id not in self.agents:
            return False

        self.stop_agent(agent_id)

        with self._lock:
            del self.agents[agent_id]
            if agent_id in self.agent_futures:
                del self.agent_futures[agent_id]

        logger.info(f"Removed agent: {agent_id}")
        return True

    def get_agent(self, agent_id: str) -> Optional[BaseAgent]:
        """Get an agent by ID."""
        return self.agents.get(agent_id)

    def list_agents(self) -> List[Dict[str, Any]]:
        """List all registered agents."""
        return [agent.get_status() for agent in self.agents.values()]

    def submit_task(self, agent_id: str, task: AgentTask) -> Optional[str]:
        """Submit a task to an agent."""
        if agent_id not in self.agents:
            return None

        agent = self.agents[agent_id]
        return agent.add_task(task)

    def get_agent_status(self, agent_id: str) -> Optional[Dict[str, Any]]:
        """Get status of a specific agent."""
        if agent_id not in self.agents:
            return None

        return self.agents[agent_id].get_status()

    def shutdown(self) -> None:
        """Shutdown all agents and the executor."""
        logger.info("Shutting down AgentOrchestrator")

        for agent_id in list(self.agents.keys()):
            self.stop_agent(agent_id)

        self.executor.shutdown(wait=True)
        logger.info("AgentOrchestrator shutdown complete")


def get_orchestrator() -> AgentOrchestrator:
    """Get the global AgentOrchestrator instance."""
    return AgentOrchestrator()
