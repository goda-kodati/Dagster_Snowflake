import string, logging, logging.handlers
import smtplib

MAILHOST = ''
FROM     = ''
TO       = ['']
SUBJECT  = 'Test Logging email from Python logging module (buffering)'

class BufferingSMTPHandler(logging.handlers.BufferingHandler):
    def __init__(self, mailhost, fromaddr, toaddrs, subject, capacity, credentials):
        logging.handlers.BufferingHandler.__init__(self, capacity)
        self.mailhost = mailhost
        self.mailport = None
        self.fromaddr = fromaddr
        self.toaddrs = toaddrs
        self.subject = subject
        self.credentials = credentials
        self.setFormatter(logging.Formatter("%(asctime)s %(levelname)-5s %(message)s"))

    def flush(self):
        if len(self.buffer) > 0:
            try:
                port = self.mailport
                if not port:
                    port = smtplib.SMTP_PORT
                smtp = smtplib.SMTP(self.mailhost, port)
                smtp.connect(self.mailhost, port)
                smtp.ehlo()
                
                # Below two not required for CAMP smtp but required for outlook smtp.
                # smtp.starttls()
                # smtp.login(self.credentials[0], self.credentials[1])
                msg = "From: %s\r\nTo: %s\r\nSubject: %s\r\n\r\n" % (self.fromaddr, self.toaddrs, self.subject)
                for record in self.buffer:
                    s = self.format(record)
                    msg = msg + s + "\r\n"
                smtp.sendmail(self.fromaddr, self.toaddrs, msg)
                smtp.quit()
            except Exception as e:
                logger = logging.getLogger()
                logger.warn(e) 
                # TODO: log exception only using file handler is possible. 
                # Check how to ignore smtp handler or this will be non-terminating loop
                # logger.warn(e) 
                # self.handleError(None)  # no particular record
            self.buffer = []

def test():
    logger = logging.getLogger("")
    logger.setLevel(logging.DEBUG)
    logger.addHandler(BufferingSMTPHandler('0', 'imro-api@campsystems.com', 'goda.kodati@campsystems.com', 
                    'Dagster Error Log', 2,  credentials=None))
    # logger.addHandler(BufferingSMTPHandler()
    # logger.addHandler(BufferingSMTPHandler()
    for i in range(12):
        logger.info("Info index = %d", i)
    logging.shutdown()

if __name__ == "__main__":
    test()