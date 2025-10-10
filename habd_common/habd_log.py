'''
*****************************************************************************
*File : habd_log.py
*Module : habd_common
*Purpose : habd data logging module (DLM) logging class
*Author : HABD Team
*Copyright : Copyright 2025, Lab to Market Innovations Private Limited
*****************************************************************************
'''

import logging
import smtplib
import threading
import email.utils
import time
import logging.handlers as handlers
#from logging.handlers import SMTPHandler


class Log:
    logger = None

    def __init__(self, module_name):
        Log.logger = logging.getLogger('HABD')
        Log.logger.setLevel(logging.WARNING)
        formatter = logging.Formatter(
            '%(levelname)s: %(asctime)s: %(threadName)s:%(module)s,%(funcName)s,%(lineno)s:  %(message)s')

        # create console handler
        ch = logging.StreamHandler()
        ch.setLevel(logging.WARNING)
        ch.setFormatter(formatter)
        Log.logger.addHandler(ch)

        # create file handler
        # Every 1 MB create a new log file
        #fh = handlers.RotatingFileHandler('../log/' + module_name + '-logfile.log', mode='a', maxBytes=1*1024*1024,
        #                                  backupCount=0)
        # Create log file every 4 hours
        #fh = handlers.TimedRotatingFileHandler('../log/' + module_name + '-logfile.log', when='S', interval=60 * 60,
        #                                     backupCount=0)
        #fh.setLevel(logging.WARNING)
        #fh.setFormatter(formatter)
        #Log.logger.addHandler(fh)

        # Create email Handler
        # Send email when critical error happens

        # host = 'smtp.office365.com'
        # port = 587
        # destEmails = ['dhanaseelant@lab-to-market.com', 'dhanaseelan.t@gmail.com']
        # fromEmail = 'testingeng@lab-to-market.com'
        # fromPass = 'Rashi1393@'
        # eh = ThreadedTlsSMTPHandler(
        #     mailhost=(host, port),
        #     fromaddr=fromEmail,
        #     toaddrs=destEmails,
        #     subject='Critical Error Has occured',
        #     credentials=(
        #         fromEmail,
        #         fromPass
        #     )
        # )
        # #
        # eh.setLevel(logging.CRITICAL)
        # eh.setFormatter(formatter)
        # Log.logger.addHandler(eh)

    def fn1(self):
        print("Function 1")
        try:
            self.fn2()
        except ZeroDivisionError:
            self.logger.critical(f"ZeroDivisionError Exception Msg", exc_info=True)

    @staticmethod
    def fn2():
        print("in function 2")
        j = 2
        k = 3
        i = 10 / 0
        m = 99
        print(f'i={i}, j={j}, k={k} l={m}')


def smtpThreadHolder(mail_host, port, username, password, from_addr, toaddrs, msg):
    smtp = None
    try:
        # print(f'mail_host: {mail_host}, port: {port}, username: {username}, password: {password}, '
        #       f'from_addr: {from_addr}, toaddrs: {toaddrs}, msg: {msg}')
        smtp = smtplib.SMTP(mail_host, port)
        if username:
            smtp.ehlo()  # for tls add this line
            smtp.starttls()  # for tls add this line
            smtp.ehlo()  # for tls add this line
        smtp.login(username, password)
        smtp.sendmail(from_addr, toaddrs, msg)
        logging.error(f"Email Sent {msg}")
    except Exception as e:
        logging.error(f"Exception occured while sending email {e}")
    finally:
        if smtp is not None:
            smtp.quit()


"""
class ThreadedTlsSMTPHandler(SMTPHandler):
    def emit(self, record):
        try:
            msg = self.format(record)
            my_msg = ("From: %s\r\nTo: %s\r\nSubject: %s\r\nDate: %s\r\n\r\n%s"
                      % (self.fromaddr, ','.join(self.toaddrs),
                          self.getSubject(record), email.utils.localtime(),
                          msg))
            thread = threading.Thread(target=smtpThreadHolder, args=(self.mailhost, self.mailport, self.fromaddr,
                                                                     self.password, self.fromaddr, self.toaddrs,
                                                                     my_msg))
            thread.daemon = True
            thread.start()
        except(KeyboardInterrupt, SystemExit):
            raise
        except:
            self.handleError(record)
"""


if __name__ == '__main__':
    my_log = Log("habd_log")
    Log.logger.debug("debug Msg")
    Log.logger.info("Info Msg")
    Log.logger.warning("warning Msg")
    Log.logger.error("error Msg")
    # Log.logger.critical("critical Msg")
    my_log.fn1()
    time.sleep(10)
