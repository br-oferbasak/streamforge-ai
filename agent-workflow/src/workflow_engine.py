import logging
import threading
from typing import Dict, List, Optional, Any
from agent import Agent
from config_loader import ConfigLoader
from communication import CommunicationManager


# Optional lineage tracking — enabled when the lineage package is importable.
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
try:
    from lineage.tracker import lineage_run, dataset as _lineage_dataset
    from lineage.emitter import default_emitter as _lineage_default_emitter
    _LINEAGE_ENABLED = True
except ImportError:
    _LINEAGE_ENABLED = False


class WorkflowEngine:
    def __init__(self, config_path: str):
        self.config_loader = ConfigLoader(config_path)
        self.workflow_config = self.config_loader.load_config()
        self.agents: Dict[str, Agent] = {}
        self.communication_manager = CommunicationManager()
        self.logger = logging.getLogger(__name__)
        self._stop_event: Optional[threading.Event] = None

    @classmethod
    def from_dict(cls, config: dict, stop_event: Optional[threading.Event] = None) -> "WorkflowEngine":
        """Construct a WorkflowEngine directly from a config dict (e.g. parsed YAML)."""
        instance = cls.__new__(cls)
        instance.workflow_config = config
        instance.agents = {}
        instance.communication_manager = CommunicationManager()
        instance.logger = logging.getLogger(__name__)
        instance._stop_event = stop_event
        return instance

    def initialize_agents(self):
        """Initialize agents based on workflow configuration"""
        for agent_config in self.workflow_config.get('agents', []):
            agent_name = agent_config['name']
            agent_type = agent_config['type']
            agent = Agent.create_agent(agent_type, agent_name, agent_config.get('config', {}))
            self.agents[agent_name] = agent
            self.communication_manager.register_agent(agent_name)
            self.logger.info(f"Initialized agent: {agent_name} (type: {agent_type})")

    def execute_workflow(self, stop_event: Optional[threading.Event] = None) -> bool:
        """Execute the workflow according to the defined steps.

        Checks *stop_event* (or the one set at construction) between steps so
        the pipeline-api can request a graceful halt without killing the thread.
        """
        _stop = stop_event or self._stop_event
        self.initialize_agents()

        steps = self.workflow_config.get('steps', [])
        for step in steps:
            if _stop and _stop.is_set():
                self.logger.info("Stop event received — halting workflow before next step")
                return False

            agent_name = step['agent']
            action = step['action']
            input_data = step.get('input', {})

            if agent_name not in self.agents:
                self.logger.error(f"Agent {agent_name} not found")
                continue

            agent = self.agents[agent_name]
            try:
                messages = self.communication_manager.get_messages(agent_name)
                if messages:
                    self.logger.info(f"Received {len(messages)} messages for agent {agent_name}")
                    input_data['messages'] = messages

                self.logger.info(f"Executing step: {action} on agent {agent_name}")

                if _LINEAGE_ENABLED:
                    result = self._execute_step_with_lineage(agent, agent_name, action, input_data, step)
                else:
                    result = agent.execute_action(action, input_data)

                self.logger.info(f"Step completed with result: {result}")

                if 'output_to' in step:
                    for next_agent, next_input_key in step['output_to'].items():
                        if next_agent in self.agents:
                            self.communication_manager.send_message(
                                agent_name, next_agent,
                                {'type': 'result', 'key': next_input_key, 'data': result},
                            )
                            self.logger.info(f"Sent result to agent {next_agent} as {next_input_key}")
            except Exception as e:
                self.logger.error(f"Error executing step: {e}")
                if step.get('on_error', 'continue') == 'stop':
                    self.logger.error("Stopping workflow due to error")
                    return False

        self.logger.info("Workflow executed successfully")
        return True

    def get_agent_status(self, agent_name: str) -> Optional[Dict[str, Any]]:
        if agent_name in self.agents:
            return self.agents[agent_name].get_status()
        return None

    def get_all_agents_status(self) -> Dict[str, Dict[str, Any]]:
        return {name: agent.get_status() for name, agent in self.agents.items()}
