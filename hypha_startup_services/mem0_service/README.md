# Mem0 Service

An AI-powered memory management service built on [Mem0](https://mem0.ai/) that provides persistent, contextual memory for AI agents and applications. This service enables agents to remember conversations, learn from interactions, and maintain context across sessions.

## Service Endpoints

### init_agent(agent_id, description=None, metadata=None, context)

Initialize an agent by creating an artifact for the agent in the workspace.

This creates a base artifact for the agent in the workspace.

**Parameters:**
- `agent_id` (str): ID of the agent
- `description` (str, optional): Optional description for the artifact
- `metadata` (dict, optional): Optional metadata for the artifact
- `context` (dict): Context from Hypha-rpc for permissions

**Returns:** None

**Example:**
```python
import asyncio
from hypha_rpc import connect_to_server

async def setup_agent():
    server = await connect_to_server({"server_url": "https://hypha.aicell.io"})
    mem0 = await server.get_service("mem0")
    
    await mem0.init_agent(
        agent_id="research-assistant",
        description="AI research assistant",
        metadata={"version": "1.0"}
    )
    # No return value - agent artifact created successfully

asyncio.run(setup_agent())
```

### init(agent_id, workspace=None, run_id=None, description=None, metadata=None, context)

Initialize a run by creating artifacts for the agent and the specific run.

This creates two artifacts: a base artifact for the agent in the workspace and a specific artifact for this run.

**Parameters:**
- `agent_id` (str): ID of the agent
- `workspace` (str, optional): Workspace for the run
- `run_id` (str, optional): ID of the run
- `description` (str, optional): Optional description for the artifacts
- `metadata` (dict, optional): Optional metadata for the artifacts
- `context` (dict): Context from Hypha-rpc for permissions

**Returns:** None

**Example:**
```python
await mem0.init(
    agent_id="research-assistant",
    run_id="session-1",
    description="Research session",
    metadata={"topic": "AI research"}
)
# No return value - run artifacts created successfully
```

### add(messages, agent_id=None, run_id=None, user_id=None, metadata=None, context)

Add memories from messages.

**Parameters:**
- `messages` (list): List of message dictionaries with 'role' and 'content'
- `agent_id` (str, optional): ID of the agent
- `run_id` (str, optional): ID of the run
- `user_id` (str, optional): ID of the user
- `metadata` (dict, optional): Additional metadata
- `context` (dict): Context from Hypha-rpc

**Returns:** Dictionary with added memories

**Example:**
```python
messages = [
    {"role": "user", "content": "I'm working on machine learning"},
    {"role": "assistant", "content": "Great! What specific area interests you?"}
]

result = await mem0.add(
    messages=messages,
    agent_id="research-assistant",
    user_id="user-123",
    metadata={"topic": "machine_learning"}
)
print(result)
# Output: {"results": [{"id": "mem_123", "message": "Added 2 memories"}]}
```

### search(query, agent_id=None, run_id=None, user_id=None, limit=100, context)

Search for memories using a query.

**Parameters:**
- `query` (str): Search query text
- `agent_id` (str, optional): ID of the agent
- `run_id` (str, optional): ID of the run 
- `user_id` (str, optional): ID of the user
- `limit` (int): Maximum number of results (default: 100)
- `context` (dict): Context from Hypha-rpc

**Returns:** Dictionary with search results

**Example:**
```python
result = await mem0.search(
    query="machine learning projects",
    agent_id="research-assistant",
    user_id="user-123",
    limit=10
)
print(result)
# Output: {"results": [{"id": "mem_456", "memory": "Working on ML project", "score": 0.95, "metadata": {...}}]}
```

### get_all(agent_id=None, run_id=None, user_id=None, filters=None, limit=100, context)

Get all memories with optional filtering.

**Parameters:**
- `agent_id` (str, optional): ID of the agent
- `run_id` (str, optional): ID of the run
- `user_id` (str, optional): ID of the user
- `filters` (dict, optional): Additional filters
- `limit` (int): Maximum number of results (default: 100)
- `context` (dict): Context from Hypha-rpc

**Returns:** Dictionary with all matching memories

**Example:**
```python
result = await mem0.get_all(
    agent_id="research-assistant",
    user_id="user-123",
    limit=50
)
print(result)
# Output: {"results": [{"id": "mem_456", "memory": "Working on ML project", "metadata": {...}}, ...]}
```

### delete_all(agent_id=None, run_id=None, user_id=None, context)

Delete all memories for specified agent/run/user.

**Parameters:**
- `agent_id` (str, optional): ID of the agent
- `run_id` (str, optional): ID of the run
- `user_id` (str, optional): ID of the user
- `context` (dict): Context from Hypha-rpc

**Returns:** Confirmation of deletion

**Example:**
```python
result = await mem0.delete_all(
    agent_id="research-assistant",
    run_id="session-1",
    user_id="user-123"
)
print(result)
# Output: {"message": "Deleted 5 memories for agent research-assistant"}
```

### set_permissions(agent_id, permissions, workspace=None, run_id=None, merge=True, context)

Set permissions for an agent in the memory service.

**Parameters:**
- `agent_id` (str): ID of the agent to set permissions for
- `permissions` (dict[str, str]): Dictionary of permissions to set
- `workspace` (str, optional): Workspace of the user setting the permissions
- `run_id` (str, optional): Run ID for the operation
- `merge` (bool): Whether to merge with existing permissions (default: True)
- `context` (dict): Context from Hypha-rpc for permissions

**Returns:** None

**Example:**
```python
await mem0.set_permissions(
    agent_id="research-assistant", 
    permissions={"read": "user123", "write": "admin"}, 
    workspace="myworkspace"
)
# No return value - permissions updated successfully
```
