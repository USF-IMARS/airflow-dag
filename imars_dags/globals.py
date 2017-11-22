class QUEUE:
    """
    basically an enum class, the string values *must* match those used in
    the puppet configuration exactly so that workers can attach to the queues.
    """
    DEFAULT = 'default'  # default queue any worker can pick up tasks from
    SAT_SCRIPTS = 'sat_scripts'  # only workers with sat-scripts installed &
    # functioning can pick up tasks from SAT_SCRIPTS
