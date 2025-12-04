"""API routes for multi-system agents."""

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from src.core.logging import get_logger
from src.services.agents import (
    AgentOrchestrator,
    AgentPriority,
    AgentTask,
    SystemConnection,
    SystemType,
    get_orchestrator,
)

logger = get_logger(__name__)
router = APIRouter()


# Request/Response Models
class CreateAgentRequest(BaseModel):
    """Request to create a new agent."""
    agent_type: str = Field(..., description="Type of agent: comparison, monitoring, or sync")
    agent_id: Optional[str] = Field(None, description="Optional custom agent ID")


class AgentResponse(BaseModel):
    """Agent status response."""
    agent_id: str
    name: str
    status: str
    created_at: str
    last_activity: str
    queue_size: int
    current_task: Optional[Dict[str, Any]] = None


class RegisterSystemRequest(BaseModel):
    """Request to register a database system."""
    name: str = Field(..., description="System display name")
    system_type: str = Field("sqlserver", description="Database type")
    host: str = Field(..., description="Database host")
    port: int = Field(1433, description="Database port")
    database: str = Field(..., description="Database name")
    username: str = Field(..., description="Username")
    password: str = Field(..., description="Password")


class SubmitTaskRequest(BaseModel):
    """Request to submit a task to an agent."""
    name: str = Field(..., description="Task name")
    description: str = Field("", description="Task description")
    priority: str = Field("NORMAL", description="Priority: LOW, NORMAL, HIGH, CRITICAL")
    payload: Dict[str, Any] = Field(default_factory=dict, description="Task payload")


class TaskResponse(BaseModel):
    """Task submission response."""
    task_id: str
    agent_id: str
    message: str


# Agent Management Endpoints
@router.post("/", response_model=AgentResponse)
async def create_agent(request: CreateAgentRequest):
    """
    Create a new agent.

    Agent types:
    - comparison: Compare data between database systems
    - monitoring: Monitor system health and metrics
    - sync: Synchronize data between systems
    """
    orchestrator = get_orchestrator()

    try:
        agent = orchestrator.create_agent(request.agent_type, request.agent_id)
        status = agent.get_status()
        return AgentResponse(**status)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/", response_model=List[AgentResponse])
async def list_agents():
    """List all registered agents."""
    orchestrator = get_orchestrator()
    agents = orchestrator.list_agents()
    return [AgentResponse(**a) for a in agents]


@router.get("/{agent_id}", response_model=AgentResponse)
async def get_agent(agent_id: str):
    """Get status of a specific agent."""
    orchestrator = get_orchestrator()
    status = orchestrator.get_agent_status(agent_id)

    if not status:
        raise HTTPException(status_code=404, detail="Agent not found")

    return AgentResponse(**status)


@router.post("/{agent_id}/start")
async def start_agent(agent_id: str):
    """Start an agent to process tasks."""
    orchestrator = get_orchestrator()

    if orchestrator.start_agent(agent_id):
        return {"message": f"Agent {agent_id} started", "status": "running"}

    raise HTTPException(status_code=404, detail="Agent not found")


@router.post("/{agent_id}/stop")
async def stop_agent(agent_id: str):
    """Stop a running agent."""
    orchestrator = get_orchestrator()

    if orchestrator.stop_agent(agent_id):
        return {"message": f"Agent {agent_id} stopped", "status": "stopped"}

    raise HTTPException(status_code=404, detail="Agent not found")


@router.delete("/{agent_id}")
async def delete_agent(agent_id: str):
    """Remove an agent."""
    orchestrator = get_orchestrator()

    if orchestrator.remove_agent(agent_id):
        return {"message": f"Agent {agent_id} removed"}

    raise HTTPException(status_code=404, detail="Agent not found")


# System Registration Endpoints
@router.post("/{agent_id}/systems")
async def register_system(agent_id: str, request: RegisterSystemRequest):
    """Register a database system with an agent."""
    orchestrator = get_orchestrator()
    agent = orchestrator.get_agent(agent_id)

    if not agent:
        raise HTTPException(status_code=404, detail="Agent not found")

    if not hasattr(agent, "register_system"):
        raise HTTPException(status_code=400, detail="Agent does not support system registration")

    try:
        system_type = SystemType(request.system_type)
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid system type: {request.system_type}")

    system = SystemConnection(
        name=request.name,
        system_type=system_type,
        host=request.host,
        port=request.port,
        database=request.database,
        username=request.username,
        password=request.password,
    )

    agent.register_system(system)

    return {
        "message": f"System {system.name} registered",
        "system_id": system.system_id,
    }


@router.get("/{agent_id}/systems")
async def list_systems(agent_id: str):
    """List systems registered with an agent."""
    orchestrator = get_orchestrator()
    agent = orchestrator.get_agent(agent_id)

    if not agent:
        raise HTTPException(status_code=404, detail="Agent not found")

    if not hasattr(agent, "systems"):
        raise HTTPException(status_code=400, detail="Agent does not have systems")

    return {
        "agent_id": agent_id,
        "systems": [s.to_dict() for s in agent.systems.values()],
    }


# Task Submission Endpoints
@router.post("/{agent_id}/tasks", response_model=TaskResponse)
async def submit_task(agent_id: str, request: SubmitTaskRequest):
    """Submit a task to an agent."""
    orchestrator = get_orchestrator()
    agent = orchestrator.get_agent(agent_id)

    if not agent:
        raise HTTPException(status_code=404, detail="Agent not found")

    try:
        priority = AgentPriority[request.priority.upper()]
    except KeyError:
        priority = AgentPriority.NORMAL

    task = AgentTask(
        name=request.name,
        description=request.description,
        priority=priority,
        payload=request.payload,
    )

    task_id = orchestrator.submit_task(agent_id, task)

    if not task_id:
        raise HTTPException(status_code=500, detail="Failed to submit task")

    return TaskResponse(
        task_id=task_id,
        agent_id=agent_id,
        message=f"Task {task.name} submitted",
    )


# Comparison Agent Specific Endpoints
@router.post("/{agent_id}/compare")
async def start_comparison(
    agent_id: str,
    source_system_id: str,
    target_system_id: str,
    tables: List[str],
):
    """Start a comparison between two systems."""
    orchestrator = get_orchestrator()
    agent = orchestrator.get_agent(agent_id)

    if not agent:
        raise HTTPException(status_code=404, detail="Agent not found")

    if agent.name != "ComparisonAgent":
        raise HTTPException(status_code=400, detail="Agent is not a ComparisonAgent")

    task = AgentTask(
        name=f"Compare {len(tables)} tables",
        description=f"Comparing tables between systems",
        priority=AgentPriority.HIGH,
        payload={
            "type": "compare",
            "source_system_id": source_system_id,
            "target_system_id": target_system_id,
            "tables": tables,
        },
    )

    task_id = agent.add_task(task)

    return {
        "task_id": task_id,
        "message": "Comparison task submitted",
        "tables": tables,
    }


# Monitoring Agent Specific Endpoints
@router.post("/{agent_id}/health-check")
async def start_health_check(agent_id: str):
    """Start a health check on all registered systems."""
    orchestrator = get_orchestrator()
    agent = orchestrator.get_agent(agent_id)

    if not agent:
        raise HTTPException(status_code=404, detail="Agent not found")

    if agent.name != "MonitoringAgent":
        raise HTTPException(status_code=400, detail="Agent is not a MonitoringAgent")

    task = AgentTask(
        name="Health Check",
        description="Check health of all systems",
        priority=AgentPriority.HIGH,
        payload={"type": "health_check"},
    )

    task_id = agent.add_task(task)

    return {"task_id": task_id, "message": "Health check task submitted"}


@router.get("/{agent_id}/metrics")
async def get_metrics(agent_id: str):
    """Get collected metrics from a monitoring agent."""
    orchestrator = get_orchestrator()
    agent = orchestrator.get_agent(agent_id)

    if not agent:
        raise HTTPException(status_code=404, detail="Agent not found")

    if not hasattr(agent, "metrics"):
        raise HTTPException(status_code=400, detail="Agent does not collect metrics")

    return {"agent_id": agent_id, "metrics": agent.metrics}


@router.get("/{agent_id}/alerts")
async def get_alerts(agent_id: str):
    """Get alerts from a monitoring agent."""
    orchestrator = get_orchestrator()
    agent = orchestrator.get_agent(agent_id)

    if not agent:
        raise HTTPException(status_code=404, detail="Agent not found")

    if not hasattr(agent, "alerts"):
        raise HTTPException(status_code=400, detail="Agent does not have alerts")

    return {"agent_id": agent_id, "alerts": agent.alerts}


# Sync Agent Specific Endpoints
@router.post("/{agent_id}/sync")
async def start_sync(
    agent_id: str,
    source_system_id: str,
    target_system_id: str,
    tables: List[str],
    mode: str = "incremental",
):
    """Start data synchronization between two systems."""
    orchestrator = get_orchestrator()
    agent = orchestrator.get_agent(agent_id)

    if not agent:
        raise HTTPException(status_code=404, detail="Agent not found")

    if agent.name != "SyncAgent":
        raise HTTPException(status_code=400, detail="Agent is not a SyncAgent")

    task = AgentTask(
        name=f"Sync {len(tables)} tables",
        description=f"Synchronizing tables ({mode} mode)",
        priority=AgentPriority.HIGH,
        payload={
            "type": "sync",
            "source_system_id": source_system_id,
            "target_system_id": target_system_id,
            "tables": tables,
            "mode": mode,
        },
    )

    task_id = agent.add_task(task)

    return {
        "task_id": task_id,
        "message": "Sync task submitted",
        "mode": mode,
        "tables": tables,
    }


@router.get("/{agent_id}/sync-history")
async def get_sync_history(agent_id: str):
    """Get synchronization history from a sync agent."""
    orchestrator = get_orchestrator()
    agent = orchestrator.get_agent(agent_id)

    if not agent:
        raise HTTPException(status_code=404, detail="Agent not found")

    if not hasattr(agent, "sync_history"):
        raise HTTPException(status_code=400, detail="Agent does not have sync history")

    return {"agent_id": agent_id, "history": agent.sync_history}
