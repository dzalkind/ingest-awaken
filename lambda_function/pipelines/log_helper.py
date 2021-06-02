import traceback

import logging

logger = logging.getLogger()
lambda_mode = True


def pad_label(label):
    return "{:<20}".format(label)


def format_log_message(msg):
    if lambda_mode:
        return msg.replace("\n", "\r")
    return msg


def get_stack_trace(error_message=''):
    if lambda_mode:
        traceback_string = traceback.format_exc().replace("\n", "\r")
    else:
        traceback_string = traceback.format_exc()

    log_message = f"""{error_message}

Stack Trace:    

{traceback_string}
"""
    return log_message

