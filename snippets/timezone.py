import pendulum

local_tz = pendulum.timezone("Europe/Amsterdam")

default_args=dict(
    start_date=datetime(2016, 1, 1, tzinfo=local_tz),
    owner='Airflow'
)

dag = DAG('my_tz_dag', default_args=default_args)
op = DummyOperator(task_id='dummy', dag=dag)
print(dag.timezone) # <Timezone [Europe/Amsterdam]

#...


local_tz = pendulum.timezone("Europe/Amsterdam")
local_tz.convert(execution_date)

# Time zone aware DAGs that use timedelta or relativedelta schedules
#  respect daylight savings time for the start date but do not adjust 
# for daylight savings time when scheduling subsequent runs. For example,
# a DAG with a start date of pendulum.create(2020, 1, 1, tz="US/Eastern") 
# and a schedule interval of timedelta(days=1) will run daily at 05:00 
# UTC regardless of daylight savings time.