from dagster import DagsterLogManager, EventLogEntry, RunRequest, job, op
from dagster_snowflake import snowflake_resource
from dagster import Definitions, asset, op, job, materialize, schedule,ScheduleEvaluationContext,DefaultScheduleStatus
import logging,datetime
from logging.handlers import TimedRotatingFileHandler


log_formatter = logging.Formatter('%(asctime)s - %(module)s - %(levelname)s - %(message)s')
file_handler = TimedRotatingFileHandler('test_log1.txt', backupCount=1000, delay=0)
file_handler.setFormatter(log_formatter)
# file_handler.setLevel(logging.INFO)

# logObj.addHandler(file_handler)
# logObj.setLevel(logging.INFO)

root_logger = logging.getLogger()
root_logger.addHandler(file_handler)
root_logger.setLevel(logging.DEBUG)

logObj = logging.getLogger(__name__)



@op(required_resource_keys={'snowflake'})
def query_one(context):
    logObj.info("query_one called")
    context.resources.snowflake.execute_query("CALL EDW.RUN_JOB_BY_NAME_SQID('p_ch_sq_id_num', 'P_SQ_ID_NUM_BACKFILL')")
    

@job(resource_defs={'snowflake': snowflake_resource})
def job_one():
    logObj.info("job_one called")
    query_one()

start_date=datetime.datetime(2023, 3, 7)

def start_date_func(context: ScheduleEvaluationContext):
    if str(context.scheduled_execution_time) > str(start_date):
        return True
    else:
        return False
    
@schedule(cron_schedule="*/2 * * * *",
           job_name="job_one",
           default_status=DefaultScheduleStatus.RUNNING,
           should_execute=start_date_func
           )
def schedule_one(context: ScheduleEvaluationContext):
 try:
    logObj.info("schedule_one called")
    job_one.execute_in_process(
        run_config=my_config
    ) 
    return RunRequest(
        run_key=None,
        run_config=my_config
    )
 except Exception as e:
        context.log.error(e)
        logObj.error(e)

# @op(required_resource_keys={'snowflake'})
# def query_two(context):
#     logObj.info("--In OP2, about to execute QUERY2--")
#     context.resources.snowflake.execute_query('SELECT SQUAWK_ID FROM EDW.SQUAWK LIMIT 5')
#     logObj.info("--QUERY2 execution completed in op2--")

# @job(resource_defs={'snowflake': snowflake_resource})
# def job_two():
#     logObj.info("--In JOB2, about to call op2--")
#     query_two()
#     logObj.info("--In JOB2, after executing OP2 query--")


# @schedule(cron_schedule="*/2 * * * *", job_name="job_two",default_status=DefaultScheduleStatus.RUNNING)
# def schedule_two(context: ScheduleEvaluationContext):
#  try:
#     logObj.info("--In SCHEDULE2, about to execute the JOB2--")
#     job_two.execute_in_process(
#         run_config=my_config
#     ) 
#     logObj.info("--In SCHEDULE2, after executing job2--") 
#     return RunRequest(
#         run_key=None,
#         run_config=my_config
#     )
#  except Exception as e:
#         context.log.error(e)
#         logObj.error(e)

# @op(required_resource_keys={'snowflake'})
# def query_three(context):
#     logObj.info("--In OP3, about to execute QUERY3--")
#     context.resources.snowflake.execute_query('SELECT ID FROM EDW.SQUAWK LIMIT 5')
#     logObj.info("--QUERY3 execution completed in op3--")

# @job(resource_defs={'snowflake': snowflake_resource})
# def job_three():
#     logObj.info("--In JOB3, about to call op3--")
#     query_three()
#     logObj.info("--In JOB3, after executing OP3 query--")


# @schedule(cron_schedule="*/2 * * * *", job_name="job_three",default_status=DefaultScheduleStatus.RUNNING)
# def schedule_three(context: ScheduleEvaluationContext):
#  try:
#     logObj.info("--In SCHEDULE3, about to execute the JOB3--")
#     job_three.execute_in_process(
#         run_config=my_config
#     ) 
#     logObj.info("--In SCHEDULE3, after executing job3--") 
#     return RunRequest(
#         run_key=None,
#         run_config=my_config
#     )
#  except Exception as e:
#         context.log.error(e)
#         logObj.error(e)

snowflake_cred= {
                        "account": "kpa19933.us-east-1",  # required
                        "user": "PBI_STAGE",  # required
                        "password": {"env": "SNOWFLAKE_STAGE_PASSWORD"},  # required
                        "database": "CAMP_STAGE",  # required
                        "warehouse": "COMPUTE_WH",  # optional, defaults to default warehouse for the account
                        "schema": "EDW",  # optional, defaults to PUBLIC
                    }
my_config={
            'resources': {
                'snowflake': {
                    'config':snowflake_cred
                }
            }
            
        }

defs = Definitions(
    jobs=[job_one],
    schedules=[schedule_one],
    resources={
        "snowflake": snowflake_resource.configured(
            snowflake_cred
        )
    },
    
)


# if __name__ == "__main__":
    # print("executing")
    # my_schedule()