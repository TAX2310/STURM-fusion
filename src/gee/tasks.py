import ee

def has_active_tasks(verbose=True):
    """
    Returns True if there are any RUNNING or READY tasks.
    Also prints diagnostics if verbose=True.
    """
    tasks = ee.batch.Task.list()

    counts = {
        "RUNNING": 0,
        "READY": 0,
        "COMPLETED": 0,
        "FAILED": 0,
        "CANCELLED": 0
    }

    for t in tasks:
        state = t.status().get('state', 'UNKNOWN')
        if state in counts:
            counts[state] += 1

    active = counts["RUNNING"] + counts["READY"]

    if verbose:
        print("\n📊 GEE Task Status:")
        print(f"RUNNING   : {counts['RUNNING']}")
        print(f"READY     : {counts['READY']}")
        print(f"COMPLETED : {counts['COMPLETED']}")
        print(f"FAILED    : {counts['FAILED']}")
        print(f"CANCELLED : {counts['CANCELLED']}")
        print(f"➡️ Active tasks: {active}")

    return active > 0