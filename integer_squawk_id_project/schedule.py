from dagster import RunRequest, job, op
from dagster_snowflake import snowflake_resource
from dagster import Definitions, asset, op, job, materialize, schedule
import logging
from logging.handlers import RotatingFileHandler, TimedRotatingFileHandler, SMTPHandler
from dagster import DagsterInstance
from graphql_relay import node_definitions
# from .smtp_handler import BufferingSMTPHandler

#from appconfig import SMTP_HOST, FROM_ADDR, TO_ADDRS, EMAIL_SUBJECT, SMTP_LOG_BUFFER_CAPACITY

# log_formatter = logging.Formatter('%(asctime)s - %(module)s - %(levelname)s - %(message)s')
# file_handler = TimedRotatingFileHandler('dagster.log', backupCount=1000, delay=0)
# file_handler.setFormatter(log_formatter)
# file_handler.setLevel(logging.INFO)
# smtp_handler = BufferingSMTPHandler(SMTP_HOST, FROM_ADDR, TO_ADDRS, 
#                      EMAIL_SUBJECT, SMTP_LOG_BUFFER_CAPACITY, credentials=None)
# smtp_handler.setLevel(logging.ERROR)
# smtp_handler.setFormatter(log_formatter)
# logObj = logging.getLogger(__name__)
# logObj.addHandler(file_handler)
# logObj.addHandler(smtp_handler)
my_config={
            'resources': {
                'snowflake': {
                    'config': {
                        "account": "kpa19933.us-east-1",  # required
                        "user": "PBI_STAGE",  # required
                        "password": {"env": "SNOWFLAKE_STAGE_PASSWORD"},  # required
                        "database": "CAMP_STAGE",  # required
                        "warehouse": "COMPUTE_WH",  # optional, defaults to default warehouse for the account
                        "schema": "EDW",  # optional, defaults to PUBLIC
                    }
                }
            }
        }


@op(required_resource_keys={'snowflake'})
def get_one(context):
    #print("getting one")
    context.resources.snowflake.execute_query('SELECT * FROM EDW.COMPLIANCE_HISTORY LIMIT 5')

@job(resource_defs={'snowflake': snowflake_resource})
def my_snowflake_job():
    get_one()


# @schedule(cron_schedule="*/2 * * * *", job_name="my_snowflake_job",mode_defs=[node_definitions(logger_defs={
#     'logObj': {
#         'config': {
#             'log_level': 'INFO',
#             'filename': 'logs/dagster.log'
#         },
#         'logger_fn': lambda init_context: logging.getLogger('logObj')
#     }
# })])
@schedule(cron_schedule="*/2 * * * *", job_name="my_snowflake_job")
def my_schedule(context):
 try:
    #logObj.info("Schedule Started!!")
    my_snowflake_job.execute_in_process(
        run_config=my_config
    )  
    return RunRequest(
        run_key=None,
        run_config=my_config
    )
 except Exception as e:
        context.log.error(e)
        #logObj.error(e)

    



defs = Definitions(
    jobs=[my_snowflake_job],
    schedules=[my_schedule],
    resources={
        "snowflake": snowflake_resource.configured(
            {
                "account": "kpa19933.us-east-1",  # required
                "user": "PBI_STAGE",  # required
                "password": {"env": "SNOWFLAKE_STAGE_PASSWORD"},  # required
                "database": "CAMP_STAGE",  # required
                "warehouse": "COMPUTE_WH",  # optional, defaults to default warehouse for the account
                "schema": "EDW",  # optional, defaults to PUBLIC
            }
        )
    },
    
)


if __name__ == "__main__":
    #print("executing")
    my_schedule()