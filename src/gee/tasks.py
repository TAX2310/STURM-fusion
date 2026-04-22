import ee
import time

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

def cancel_all_tasks(verbose=True):
    """
    Cancels all READY or RUNNING Earth Engine tasks.
    """

    tasks = ee.batch.Task.list()

    cancelled = 0
    skipped = 0

    for t in tasks:
        status = t.status()
        state = status['state']
        desc = status.get('description', 'N/A')

        if state in ['READY', 'RUNNING']:
            t.cancel()
            cancelled += 1
            if verbose:
                print(f"❌ Cancelled: {desc} ({state})")
        else:
            skipped += 1
            if verbose:
                print(f"⏭️ Skipped: {desc} ({state})")

    print("\n📊 Summary:")
    print(f"Cancelled: {cancelled}")
    print(f"Skipped (already done): {skipped}")

def wait_for_task(task, sleep=10, verbose=True):
    """
    Wait until a GEE task completes.
    """
    while True:
        status = task.status()
        state = status["state"]

        if verbose:
            print(f"Task state: {state}", end="", flush=True)

        if state in ["COMPLETED", "FAILED", "CANCELLED"]:
            return status

        time.sleep(sleep)