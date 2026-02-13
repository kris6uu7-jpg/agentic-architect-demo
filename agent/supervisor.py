import mlflow
from databricks import agents
from tools.branch_logic import trigger_sandbox_branch

# Logic for the Mosaic AI Agentic Loop
def deploy_autonomous_supervisor():
    """Logs the repair agent as a governed Mosaic AI model."""

    # Clear instructions for the LLM
    SYSTEM_INSTRUCTIONS = """
    You are the Autonomous Data Supervisor.
    1. Monitor logs for schema drift or DLT pipeline failures.
    2. If a failure occurs, use 'trigger_sandbox_branch' for isolation.
    3. Refactor the failing SQL/Python code inside that branch.
    4. Validate the branch and notify the human architect for merge.
    """

    with mlflow.start_run():
        # Registering the model in Unity Catalog for governance
        logged_agent = agents.log_model(
            model="databricks-meta-llama-3-3-70b-instruct",
            endpoint_name="supervisor-engine-v1",
            tools=[trigger_sandbox_branch],
            instructions=SYSTEM_INSTRUCTIONS
        )
    return logged_agent
