import celery
import irods.exception

class MangoFlowTask(celery.Task):
    """Custom implementation of a celery Task class"""

    auto_retry_for = (irods.exception.CAT_CONNECT_ERR, irods.exception.NetworkException)
    retry_backoff = True  # exponential backoff
    retry_backoff_max = 1800  # 30 min is the maximum to wait between retries, but it will try for much longer in total
    # retry_jitter = True  # default, avoid thermonuclear reactions

        

