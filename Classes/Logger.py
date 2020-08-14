import functools 
import logging
import logging.handlers
import os 

filename = None
error_log = None 
info_log = None 
df_log = None
log_all_df = None 

def create_abs_path(rel_path): 
    path_to_data = os.getcwd() + os.sep + os.pardir
    return (os.path.normpath(path_to_data + rel_path))

def create_info_log(path):
    logger = logging.getLogger('LogInfo')
    logger.setLevel(logging.INFO)

    file_handler = logging.FileHandler(f'{path}.info.log',mode='w')

    log_format = '%(levelname)s %(asctime)s %(message)s'
    formatter = logging.Formatter(log_format) 
    file_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    return logger 

def create_error_log(path):
    logger = logging.getLogger('LogError')
    logger.setLevel(logging.ERROR)

    file_handler = logging.FileHandler(f'{path}.error.log',mode='w')

    log_format = '%(levelname)s %(asctime)s %(message)s'
    formatter = logging.Formatter(log_format) 
    file_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    return logger  

def create_log(log_type,path):

    if log_type == 'DF' or log_type == 'INFO':
        logger = logging.getLogger('LogInfo')
        logger.setLevel(logging.INFO)
        log_path = f'{path}.df.info.log' if log_type == 'DF' else f'{path}.info.log'
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
    info_log = create_info_log(create_abs_path(filename)) 
    error_log = create_error_log(create_abs_path(filename))
    df_log = create_log(log_type='DF',path=path)

def log(name,**kwargs):
    # if info_log is None or error_log is None:
    #     raise('Please set the filename of the log file')
    success_message = kwargs.get('message',f'{name} completed successfully')
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
                    df_log.info(f'Raw DF before {name} process\n{self.__raw_df__.to_string()}')
                # Executing wrapped function 
                f = func(self,*args,**kwargs)

                if f is not None:
                    wrapper_fname, message, data = f 
                    success_message += f' and {wrapper_fname}'
                    if message is not None:
                        success_message += f' returned a message of {message}'
                    if data is not None:
                        if message is None:
                            success_message += f' returned data {data}'
                        success_message += f' with data {data}'
                # Logging success message
                
                info_log.info(success_message)
                if log_df is True:
                    df_log.info(f'Raw DF after {name} process\n{self.__raw_df__.to_string()}')
            except Exception:
                message = f"Error in process {name}\n"
                error_log.exception(message)
                pass
        return wrapper 
    return container
