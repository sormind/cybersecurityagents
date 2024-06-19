import os
from crewai import Agent, Task, Crew, Process
from kafka import KafkaConsumer
import json

# Environment Variables
os.environ["OPENAI_API_BASE"] = 'https://api.groq.com/openai/v1'
os.environ["OPENAI_MODEL_NAME"] = 'llama3-8b-8192'
os.environ["OPENAI_API_KEY"] = 'your_groq_api_key'

def consume_data(topic):
    consumer = KafkaConsumer(topic, bootstrap_servers=['localhost:9092'],
                             value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    data = next(consumer).value
    return data

# Define Agents
network_traffic_agent = Agent(
    role="network traffic agent",
    goal="Analyze network traffic patterns",
    backstory="Identifies suspicious network activity.",
    verbose=True,
    allow_delegation=False
)

system_log_agent = Agent(
    role="system log agent",
    goal="Examine system logs for unusual behavior",
    backstory="Detects anomalies in system logs.",
    verbose=True,
    allow_delegation=False
)

endpoint_agent = Agent(
    role="endpoint agent",
    goal="Monitor endpoint devices for malware and unauthorized access",
    backstory="Identifies endpoint security threats.",
    verbose=True,
    allow_delegation=False
)

file_integrity_agent = Agent(
    role="file integrity agent",
    goal="Check for file system modifications and data tampering",
    backstory="Detects file system anomalies.",
    verbose=True,
    allow_delegation=False
)

user_behavior_agent = Agent(
    role="user behavior agent",
    goal="Analyze user behavior patterns for insider threats",
    backstory="Identifies user behavior anomalies.",
    verbose=True,
    allow_delegation=False
)

orchestrator_agent = Agent(
    role="orchestrator agent",
    goal="Correlate data from all agents and detect anomalies",
    backstory="Makes a final determination on anomaly detection.",
    verbose=True,
    allow_delegation=False
)

# Define Tasks
analyze_network_traffic = Task(
    description="Analyze network traffic patterns",
    agent=network_traffic_agent,
    expected_output="suspicious or normal network traffic",
    file_path=None,
    prompt=lambda: f"Study the network traffic patterns and identify any suspicious activity: '{consume_data('network_traffic')}'"
)

examine_system_logs = Task(
    description="Examine system logs for unusual behavior",
    agent=system_log_agent,
    expected_output="unusual or normal system log activity",
    file_path=None,
    prompt=lambda: f"Analyze the system logs and identify any unusual behavior or anomalies: '{consume_data('system_logs')}'"
)

monitor_endpoint_devices = Task(
    description="Monitor endpoint devices for malware and unauthorized access",
    agent=endpoint_agent,
    expected_output="malware detected or clean endpoint device",
    file_path=None,
    prompt=lambda: f"Monitor the endpoint devices and identify any malware or unauthorized access: '{consume_data('endpoint_devices')}'"
)

check_file_integrity = Task(
    description="Check for file system modifications and data tampering",
    agent=file_integrity_agent,
    expected_output="tampered or intact file system",
    file_path=None,
    prompt=lambda: f"Check the file system for any modifications or data tampering: '{consume_data('file_system')}'"
)

analyze_user_behavior = Task(
    description="Analyze user behavior patterns for insider threats",
    agent=user_behavior_agent,
    expected_output="insider threat or normal user behavior",
    file_path=None,
    prompt=lambda: f"Analyze the user behavior patterns and identify any insider threats: '{consume_data('user_behavior')}'"
)

correlate_data = Task(
    description="Correlate data from all agents and detect anomalies",
    agent=orchestrator_agent,
    expected_output="anomaly detected or normal activity",
    file_path=None,
    prompt=lambda: f"Correlate the data from all agents and identify any anomalies or suspicious activity: '{consume_data('correlated_data')}'"
)

# Create and Execute Crew
cyber_security_crew = Crew(
    agents=[network_traffic_agent, system_log_agent, endpoint_agent, file_integrity_agent, user_behavior_agent, orchestrator_agent],
    tasks=[analyze_network_traffic, examine_system_logs, monitor_endpoint_devices, check_file_integrity, analyze_user_behavior, correlate_data],
    verbose=2,
    process=Process.sequential
)

output = cyber_security_crew.kickoff()
print(output)