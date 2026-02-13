# COMMAND ----------
# MAGIC %md #  Data + AI Summit 2026: The Agentic Architect
# MAGIC ### Autonomous Pipeline Remediation with Mosaic AI & Lakebase
# MAGIC This notebook demonstrates how v4c.ai uses **Agent Bricks** to automate 80% of Tier-1 data support.

# COMMAND ----------
# MAGIC %pip install databricks-sdk mlflow databricks-agents --upgrade
# MAGIC dbutils.library.restartPython()

# COMMAND ----------
# MAGIC %md ## 1. Environmental Setup
# MAGIC Authenticating with Unity Catalog and loading environment configs.

from databricks.sdk import WorkspaceClient
import yaml

w = WorkspaceClient()
# Load config for the 'demo-autonomous-warehouse' project
with open('../config.yaml', 'r') as f:
    config = yaml.safe_load(f)

print(f"Targeting Project: {config['workspace']['lakebase_project']}")

# COMMAND ----------
# MAGIC %md ## 2. The Tools: Lakebase Branching API
# MAGIC We import the specialized 'Hands' of our agent from the modular toolset.

import sys
sys.path.append('..')
from tools.branch_logic import trigger_sandbox_branch

# COMMAND ----------
# MAGIC %md ## 3. The Brain: Mosaic AI Agentic Loop
# MAGIC Defining the reasoning engine that turns 3:00 AM errors into fixes.

import mlflow
from databricks import agents

# Define the 2026 'Self-Healing' instructions
SYSTEM_PROMPT = f"""
You are the v4c.ai Supervisor Agent.
1. Watch for 'Schema Mismatch' or 'Pipeline Failure' logs.
2. Use 'trigger_sandbox_branch' to isolate the data immediately.
3. Suggest a code fix using {config['agent_settings']['model_endpoint']}.
4. Request human approval before merging to the Gold Layer.
"""

with mlflow.start_run():
    # Registering the agent as a governed model in Unity Catalog
    logged_agent = agents.log_model(
        model=config['agent_settings']['model_endpoint'],
        endpoint_name="v4c-repair-engine",
        tools=[trigger_sandbox_branch],
        instructions=SYSTEM_PROMPT
    )

# COMMAND ----------
# MAGIC %md ## 4. Live Simulation
# MAGIC Watch as the agent detects a simulated Silver-layer failure.

failure = "FAILED: Column 'cust_id' expected INT, found STRING in table 'bronze_sales'."
print(f"DETECTION: {failure}")

# The Agent triggers the sandbox branch automatically
sandbox_status = trigger_sandbox_branch("fix_v1_summit_demo")
print(sandbox_status)
print("AGENT STATUS: Proposing 'CAST(cust_id AS INT)' refactor. Awaiting approval...")
