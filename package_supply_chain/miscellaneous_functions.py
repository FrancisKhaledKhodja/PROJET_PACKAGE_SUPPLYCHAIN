from time import perf_counter
#from package_supply_chain.my_loguru import logger


def get_execution_time(func):
    '''
    Calculate the execution time of a function
    '''
    def wrapper(*args, **kargs):        
        t0 = perf_counter()
        result = func(*args, **kargs)
        t1 = perf_counter()
        #logger.info("Temps d'execution: {} secondes".format(str(round(t1 - t0, 0))))
        return result
    return wrapper