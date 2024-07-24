import celery
import irods.exception

class MangoFlowTask(celery.Task):
    """Custom implementation of a celery Task class"""

    auto_retry_for = (irods.exception.CAT_CONNECT_ERR, irods.exception.NetworkException)
    retry_backoff = True  # exponential backoff
    retry_backoff_max = 1800  # 30 min is the maximum to wait between retries, but it will try for much longer in total
    # retry_jitter = True  # default, avoid thermonuclear reactions


@celery.shared_task(name="mango_flow.irods.chain.path", bind = True)
def mango_flow_chain_path_tasks(self, path: str, actions: list):
    """"""
    start_action = actions.pop(0)
    task = celery.signature(start_action['name'], args = [path], kwargs = start_action['parameters'])
    for action in actions:
        # the rest are signature partials, the output of the first task will be injected as the first arg in the next
        task.link(celery.signature(action['name']), kwargs = action['parameters'])
    
    task.apply_async()
        

