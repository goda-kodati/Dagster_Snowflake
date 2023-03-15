from dagster import DagsterLogManager, EventLogEntry, RunRequest, job, op
from dagster_snowflake import snowflake_resource
from dagster import Definitions, asset, op, job, materialize, schedule,ScheduleEvaluationContext,DefaultScheduleStatus
import logging,datetime
from logging.handlers import TimedRotatingFileHandler

import pendulum


log_formatter = logging.Formatter('%(asctime)s - %(module)s - %(levelname)s - %(message)s')
file_handler = TimedRotatingFileHandler('test_log1.txt', backupCount=1000, delay=0)
file_handler.setFormatter(log_formatter)

root_logger = logging.getLogger()
root_logger.addHandler(file_handler)
root_logger.setLevel(logging.WARN)

logObj = logging.getLogger(__name__)

@op(required_resource_keys={'snowflake'})
def current_status_backfill_op(context):
    logObj.info("query_one called")
    context.resources.snowflake.execute_query("CALL EDW.RUN_JOB_BY_NAME_SQID('p_cs_sq_id_num', 'P_CS_SQ_ID_NUM_BACKFILL')")
    

@job(resource_defs={'snowflake': snowflake_resource})
def current_status_backfill_job():
    logObj.info("job_one called")
    current_status_backfill_op()

start_date=pendulum.datetime(year=2023, month=4, day=14, hour=1)

def start_date_func(context: ScheduleEvaluationContext):
    # return False
    logObj.warn('from logObj Scheduled_execution time_zone = : {0}, dtpe:{1}'.format(context.scheduled_execution_time.timezone_name, type(context.scheduled_execution_time)))
    logObj.warn('from logObj Scheduled_execution time = : {0}, dtpe:{1}'.format(context.scheduled_execution_time, type(context.scheduled_execution_time)))
    logObj.warn('from logObj start_date time_zone = : {0}, dtpe:{1}'.format(start_date.timezone_name,type(start_date)))
    logObj.warn('from logObj start_date = : {0}, dtpe:{1}'.format(start_date, type(start_date)))

    if context.scheduled_execution_time.date() >= start_date.date():
        if context.scheduled_execution_time.timestamp() >= start_date.timestamp():
         logObj.info(True)
         return True
    else:
        logObj.info(False)
        return False
    
@schedule(cron_schedule="*/3 * * * *",
           job_name="current_status_backfill_job",
           default_status=DefaultScheduleStatus.RUNNING,
           should_execute=start_date_func
           )
def current_status_backfill_schedule(context: ScheduleEvaluationContext):
 try:
    logObj.info("schedule_one called")
    current_status_backfill_job.execute_in_process(
        run_config=my_config
    ) 
    return RunRequest(
        run_key=None,
        run_config=my_config
    )
 except Exception as e:
        context.log.error(e)
        logObj.error(e)

@op(required_resource_keys={'snowflake'})
def compliance_history_backfill_op(context):
    logObj.info("query_one called")
    context.resources.snowflake.execute_query("CALL EDW.RUN_JOB_BY_NAME_SQID('p_ch_sq_id_num', 'P_CH_SQ_ID_NUM_BACKFILL')")
    

@job(resource_defs={'snowflake': snowflake_resource})
def compliance_history_backfill_job():
    logObj.info("job_one called")
    compliance_history_backfill_op()

# start_date=datetime.datetime(year=2023, month=3, day=13, hour=2, minute=45)

# def start_date_func(context: ScheduleEvaluationContext):
#     logObj.info("context.scheduled_execution_time="+context.scheduled_execution_time)
#     if str(context.scheduled_execution_time) > str(start_date):
#         return True
#     else:
#         return False
    
@schedule(cron_schedule="0 */1 * * *",
           job_name="compliance_history_backfill_job",
           default_status=DefaultScheduleStatus.RUNNING,
           should_execute=start_date_func
           )
def compliance_history_backfill_schedule(context: ScheduleEvaluationContext):
 try:
    logObj.info("schedule_one called")
    compliance_history_backfill_job.execute_in_process(
        run_config=my_config
    ) 
    return RunRequest(
        run_key=None,
        run_config=my_config
    )
 except Exception as e:
        context.log.error(e)
        logObj.error(e)

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
    jobs=[current_status_backfill_job,compliance_history_backfill_job],
    schedules=[current_status_backfill_schedule,compliance_history_backfill_schedule],
    resources={
        "snowflake": snowflake_resource.configured(
            snowflake_cred
        )
    },
    
)


# if __name__ == "__main__":
    # print("executing")
    # my_schedule()
