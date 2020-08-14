import functools 
import logging
import logging.handlers
import os 
import time

filename = None
error_log = None 
info_log = None 
df_log = None
log_all_df = None 

def create_abs_path(rel_path): 
    path_to_data = os.getcwd() + os.sep + os.pardir
    return (os.path.normpath(path_to_data + rel_path))
 

def create_log(log_type,path):

    if log_type == 'INFO':
        logger = logging.getLogger('LogInfo')
        logger.setLevel(logging.INFO)
        log_path = f'{path}.info.log'
    elif log_type == 'DF':
        logger = logging.getLogger('LogDebug')
        logger.setLevel(logging.DEBUG)
        log_path = f'{path}.debug.log'
    else:
        logger = logging.getLogger('LogError')
        logger.setLevel(logging.ERROR)
        log_path = f'{path}.error.log'

    file_handler = logging.FileHandler(log_path,mode='w')

    log_format = '%(levelname)s %(asctime)s %(message)s'
    formatter = logging.Formatter(log_format) 
    file_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    return logger  

def init(filename):
    global error_log, info_log, df_log, log_all_df
    path = create_abs_path(filename)
    info_log = create_log(log_type='INFO',path=path)
    error_log = create_log(log_type='ERROR',path=path)
    df_log = create_log(log_type='DF',path=path)

def message(msg):
    global info_log
    info_log.info(msg)

def log(name,**kwargs):
    # if info_log is None or error_log is None:
    #     raise('Please set the filename of the log file')
    success_message = kwargs.get('message',f'{name} completed')
    wrapper = kwargs.get('wrapper',False)
    log_df = kwargs.get('df',False)

    def container(func):
        @functools.wraps(func)
        def wrapper(self,*args,**kwargs):
            global error_log, info_log, df_log
            nonlocal success_message, name, wrapper
            try:
                # Logging df prior to function execution
                if log_df is True:
                    df_log.debug(f'Raw DF before {name} process\n{self.__raw_df__.to_string()}')
                # Executing wrapped function 

                start = time.perf_counter()
                f = func(self,*args,**kwargs)
                time_taken = f' TIME TAKEN: {time.perf_counter() - start}s'

                data_returned = '' 

                if f is not None:
                    data_returned = f' RETURNED: {f}'
                
                info_log.info(success_message + data_returned + time_taken)

                if log_df is True:
                    df_log.debug(f'Raw DF after {name} process\n{self.__raw_df__.to_string()}')
            except Exception:
                message = f"Process {name}\n"
                error_log.exception(message)
                pass
        return wrapper 
    return container
