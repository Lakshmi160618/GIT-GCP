from airflow import DAG
from airflow.operators.email_operator import EmailOperator
from datetime import datetime, timedelta
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.models import Variable

slack_channel = Variable.get('slack_channel')


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 5, 3), 
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'send_email_with_slack_integration123',
    default_args=default_args,
    description='Send email with Slack integration',
    schedule_interval=timedelta(days=1),
)

send_email = SlackWebhookOperator(
    task_id='send_email',
    slack_webhook_conn_id='slack',
	webhook_token='https://hooks.slack.com/services/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',
	message='Hi This is from Airflow through slack channel',
    channel=slack_channel,
    username='Airflow',
    icon_url='https://raw.githubusercontent.com/apache/airflow/main/airflow/www/static/pin_100.png',
    dag=dag,
)

send_email

