import ee

def has_active_tasks(verbose=False):
    """
    Returns True if there are any RUNNING or READY tasks.
    Also prints diagnostics if verbose=True.
    """
    tasks = ee.batch.Task.list()

    if verbose:
        running_count = sum(t.status()['state'] == 'RUNNING' for t in tasks)
        print("\n📊 GEE Task Status:")
        print(f"RUNNING   : {running_count}")
        return running_count > 0
    else:
        return any(t.status()['state'] in ['RUNNING', 'READY'] for t in tasks)

    