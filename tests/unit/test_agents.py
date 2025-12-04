"""Tests for multi-system agents module."""

import pytest
import time
import threading

from src.services.agents import (
    AgentOrchestrator,
    AgentPriority,
    AgentStatus,
    AgentTask,
    BaseAgent,
    ComparisonAgent,
    MonitoringAgent,
    SyncAgent,
    SystemConnection,
    SystemType,
)


class TestAgentTask:
    """Tests for AgentTask dataclass."""

    def test_task_creation(self):
        """Test task creation with default values."""
        task = AgentTask(name="Test Task")
        assert task.name == "Test Task"
        assert task.status == AgentStatus.IDLE
        assert task.priority == AgentPriority.NORMAL
        assert task.task_id is not None

    def test_task_with_priority(self):
        """Test task with custom priority."""
        task = AgentTask(name="High Priority", priority=AgentPriority.CRITICAL)
        assert task.priority == AgentPriority.CRITICAL

    def test_task_to_dict(self):
        """Test task serialization."""
        task = AgentTask(name="Test", payload={"key": "value"})
        data = task.to_dict()
        assert data["name"] == "Test"
        assert data["payload"] == {"key": "value"}
        assert "task_id" in data


class TestSystemConnection:
    """Tests for SystemConnection dataclass."""

    def test_system_creation(self):
        """Test system connection creation."""
        system = SystemConnection(
            name="Test DB",
            system_type=SystemType.SQL_SERVER,
            host="localhost",
            port=1433,
            database="test_db",
            username="user",
            password="pass",
        )
        assert system.name == "Test DB"
        assert system.system_type == SystemType.SQL_SERVER
        assert system.is_active is True

    def test_system_to_dict_excludes_password(self):
        """Test system serialization excludes password."""
        system = SystemConnection(
            name="Test",
            host="localhost",
            database="db",
            username="user",
            password="secret",
        )
        data = system.to_dict()
        assert "password" not in data
        assert data["username"] == "user"


class TestComparisonAgent:
    """Tests for ComparisonAgent."""

    def test_agent_creation(self):
        """Test comparison agent creation."""
        agent = ComparisonAgent()
        assert agent.name == "ComparisonAgent"
        assert agent.status == AgentStatus.IDLE

    def test_register_system(self):
        """Test registering a system."""
        agent = ComparisonAgent()
        system = SystemConnection(
            name="Test DB",
            host="localhost",
            database="test",
            username="user",
            password="pass",
        )
        agent.register_system(system)
        assert system.system_id in agent.systems

    def test_add_task(self):
        """Test adding a task to agent."""
        agent = ComparisonAgent()
        task = AgentTask(name="Compare Tables")
        task_id = agent.add_task(task)
        assert task_id == task.task_id

    def test_execute_comparison_task(self):
        """Test executing a comparison task."""
        agent = ComparisonAgent()

        # Register two systems
        source = SystemConnection(name="Source", host="localhost", database="source", username="user", password="pass")
        target = SystemConnection(name="Target", host="localhost", database="target", username="user", password="pass")
        agent.register_system(source)
        agent.register_system(target)

        task = AgentTask(
            name="Test Compare",
            payload={
                "type": "compare",
                "source_system_id": source.system_id,
                "target_system_id": target.system_id,
                "tables": ["table1", "table2"],
            },
        )

        result = agent.execute_task(task)
        assert result["tables_compared"] == 2
        assert "comparisons" in result

    def test_get_status(self):
        """Test getting agent status."""
        agent = ComparisonAgent()
        status = agent.get_status()
        assert status["name"] == "ComparisonAgent"
        assert status["status"] == "idle"
        assert "agent_id" in status


class TestMonitoringAgent:
    """Tests for MonitoringAgent."""

    def test_agent_creation(self):
        """Test monitoring agent creation."""
        agent = MonitoringAgent()
        assert agent.name == "MonitoringAgent"

    def test_health_check(self):
        """Test health check task."""
        agent = MonitoringAgent()
        system = SystemConnection(name="Test", host="localhost", database="db", username="user", password="pass")
        agent.register_system(system)

        task = AgentTask(name="Health Check", payload={"type": "health_check"})
        result = agent.execute_task(task)

        assert "systems" in result
        assert len(result["systems"]) == 1

    def test_collect_metrics(self):
        """Test metrics collection."""
        agent = MonitoringAgent()
        system = SystemConnection(name="Test", host="localhost", database="db", username="user", password="pass")
        agent.register_system(system)

        task = AgentTask(name="Collect Metrics", payload={"type": "collect_metrics"})
        result = agent.execute_task(task)

        assert "metrics" in result
        assert len(agent.metrics[system.system_id]) == 1

    def test_check_alerts(self):
        """Test alert checking."""
        agent = MonitoringAgent()
        task = AgentTask(name="Check Alerts", payload={"type": "check_alerts"})
        result = agent.execute_task(task)
        assert "alerts" in result


class TestSyncAgent:
    """Tests for SyncAgent."""

    def test_agent_creation(self):
        """Test sync agent creation."""
        agent = SyncAgent()
        assert agent.name == "SyncAgent"

    def test_sync_task(self):
        """Test sync task execution."""
        agent = SyncAgent()

        source = SystemConnection(name="Source", host="localhost", database="source", username="user", password="pass")
        target = SystemConnection(name="Target", host="localhost", database="target", username="user", password="pass")
        agent.register_system(source)
        agent.register_system(target)

        task = AgentTask(
            name="Sync Tables",
            payload={
                "type": "sync",
                "source_system_id": source.system_id,
                "target_system_id": target.system_id,
                "tables": ["table1"],
                "mode": "incremental",
            },
        )

        result = agent.execute_task(task)
        assert result["mode"] == "incremental"
        assert result["tables_synced"] == 1
        assert len(agent.sync_history) == 1

    def test_generate_sync_script(self):
        """Test sync script generation."""
        agent = SyncAgent()
        task = AgentTask(name="Generate Script", payload={"type": "generate_script"})
        result = agent.execute_task(task)
        assert "script_type" in result

    def test_validate_sync(self):
        """Test sync validation."""
        agent = SyncAgent()
        task = AgentTask(name="Validate", payload={"type": "validate"})
        result = agent.execute_task(task)
        assert result["validation_status"] == "passed"


class TestAgentOrchestrator:
    """Tests for AgentOrchestrator."""

    @pytest.fixture
    def orchestrator(self):
        """Create fresh orchestrator for each test."""
        orch = AgentOrchestrator()
        # Clear existing agents
        for agent_id in list(orch.agents.keys()):
            orch.remove_agent(agent_id)
        return orch

    def test_create_comparison_agent(self, orchestrator):
        """Test creating comparison agent."""
        agent = orchestrator.create_agent("comparison")
        assert agent.name == "ComparisonAgent"
        assert agent.agent_id in orchestrator.agents

    def test_create_monitoring_agent(self, orchestrator):
        """Test creating monitoring agent."""
        agent = orchestrator.create_agent("monitoring")
        assert agent.name == "MonitoringAgent"

    def test_create_sync_agent(self, orchestrator):
        """Test creating sync agent."""
        agent = orchestrator.create_agent("sync")
        assert agent.name == "SyncAgent"

    def test_create_invalid_agent_type(self, orchestrator):
        """Test creating invalid agent type raises error."""
        with pytest.raises(ValueError):
            orchestrator.create_agent("invalid_type")

    def test_list_agents(self, orchestrator):
        """Test listing agents."""
        orchestrator.create_agent("comparison")
        orchestrator.create_agent("monitoring")
        agents = orchestrator.list_agents()
        assert len(agents) == 2

    def test_get_agent(self, orchestrator):
        """Test getting agent by ID."""
        agent = orchestrator.create_agent("comparison")
        retrieved = orchestrator.get_agent(agent.agent_id)
        assert retrieved == agent

    def test_remove_agent(self, orchestrator):
        """Test removing agent."""
        agent = orchestrator.create_agent("comparison")
        result = orchestrator.remove_agent(agent.agent_id)
        assert result is True
        assert agent.agent_id not in orchestrator.agents

    def test_submit_task(self, orchestrator):
        """Test submitting task to agent."""
        agent = orchestrator.create_agent("comparison")
        task = AgentTask(name="Test Task")
        task_id = orchestrator.submit_task(agent.agent_id, task)
        assert task_id is not None


class TestAgentPriority:
    """Tests for task priority handling."""

    def test_priority_ordering(self):
        """Test tasks are processed by priority."""
        agent = ComparisonAgent()

        low_task = AgentTask(name="Low", priority=AgentPriority.LOW)
        high_task = AgentTask(name="High", priority=AgentPriority.HIGH)
        critical_task = AgentTask(name="Critical", priority=AgentPriority.CRITICAL)

        # Add in reverse priority order
        agent.add_task(low_task)
        agent.add_task(high_task)
        agent.add_task(critical_task)

        # Verify queue has all tasks
        assert agent._task_queue.qsize() == 3


class TestAgentCallbacks:
    """Tests for agent callback functionality."""

    def test_register_callback(self):
        """Test callback registration."""
        agent = ComparisonAgent()
        callback_called = []

        def my_callback(task):
            callback_called.append(task)

        agent.register_callback("on_task_start", my_callback)
        assert len(agent._callbacks["on_task_start"]) == 1

    def test_callback_triggered(self):
        """Test callbacks are triggered on events."""
        agent = ComparisonAgent()
        status_changes = []

        def on_status_change(old, new):
            status_changes.append((old, new))

        agent.register_callback("on_status_change", on_status_change)

        # Trigger status change
        agent._set_status(AgentStatus.RUNNING)

        assert len(status_changes) == 1
        assert status_changes[0] == (AgentStatus.IDLE, AgentStatus.RUNNING)
