from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime


def dummy_dag():
    """
    A simple example of using Airflow's scheduler

    :return:
    """
    with DAG('dummy_dag', start_date=datetime.today()) as dag:
        op1 = DummyOperator(task_id='Task_1')
        op2 = DummyOperator(task_id='Task_2')
        op3 = DummyOperator(task_id='Task_3')
        op4 = DummyOperator(task_id='Task_4')
        op5 = DummyOperator(task_id='Task_5')
        op6 = DummyOperator(task_id='Task_6')
        op1.set_downstream([op2, op3])
        op2.set_downstream([op4, op5, op6])
        op3.set_downstream([op4, op5, op6])
