
from dagster import Definitions, asset
from logging.handlers import TimedRotatingFileHandler, SMTPHandler
from smtp_handler import BufferingSMTPHandler
from email.message import EmailMessage
import logging
from config import my_snowflake_resources

log_formatter = logging.Formatter('%(asctime)s - %(module)s - %(levelname)s - %(message)s')
file_handler = TimedRotatingFileHandler('custom_logs.txt', backupCount=1000, delay=0)
file_handler.setFormatter(log_formatter)
file_handler.setLevel(logging.INFO)
# smtp_handler = BufferingSMTPHandler(SMTP_HOST, FROM_ADDR, TO_ADDRS, 
#                     EMAIL_SUBJECT, SMTP_LOG_BUFFER_CAPACITY, credentials=None)
# smtp_handler.setLevel(logging.ERROR)
# smtp_handler.setFormatter(log_formatter)
logObj = logging.getLogger(__name__)
logObj.addHandler(file_handler)
@asset(required_resource_keys={"snowflake"})
def small_aircraft(context):
    
    try:
        logObj.info(e)
        #logObj.info(context.log.info)
        #raise Exception("OOPS! Raised exception explicitly!!")
        return context.resources.snowflake.execute_query(
            (
                "CALL EDW.RUN_JOB_BY_NAME_SQID('p_ch_sq_id_num', 'P_SQ_ID_NUM_BACKFILL')"
            ),
            fetch_results=True,
            use_pandas_result=True
        )
    except Exception as e:
        context.log.error(e)
        
        #raise RetryRequested(max_retries=3,seconds_to_wait=10) from e
       
defs = Definitions(assets=[small_aircraft], resources=my_snowflake_resources)





# if __name__ == '__main__':
#     test_sf()
