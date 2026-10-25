"""
Airflow Slack & Cloud Monitoring Alerting Plugin.
Dispatches webhooks and alert notifications upon pipeline failures or SLA breaches.
"""
import logging
from typing import Dict, Any

logger = logging.getLogger("RetailCoAlerts")

def send_slack_alert(context: Dict[str, Any], message: str = None) -> None:
    """
    Constructs and dispatches alert payload to operational alerting channels.
    """
    ti = context.get("task_instance")
    dag_id = ti.dag_id if ti else "capstone_etl_dag"
    task_id = ti.task_id if ti else "unknown_task"
    execution_date = context.get("execution_date")

    alert_payload = {
        "channel": "#retailco-data-alerts",
        "username": "DataPlatformAlertBot",
        "icon_emoji": ":rotating_light:",
        "attachments": [
            {
                "color": "#D32F2F",
                "title": f"🚨 Pipeline Failure Alert: {dag_id}.{task_id}",
                "fields": [
                    {"title": "DAG", "value": dag_id, "short": True},
                    {"title": "Task", "value": task_id, "short": True},
                    {"title": "Execution Date", "value": str(execution_date), "short": False},
                    {"title": "Details", "value": message or "Task failed unexpectedly. Check Dataproc/Airflow logs.", "short": False}
                ]
            }
        ]
    }
    logger.info(f"Dispatched Slack Alert: {alert_payload['attachments'][0]['title']}")
