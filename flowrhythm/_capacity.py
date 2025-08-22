class Capacity:
    def __init__(self):
        self.initial_workers = 3
        self.min_workers = 1
        self.max_workers = 5
        self.queue_length = 10


class UtilizationCapacity(Capacity):
    def __init__(self):
        super().__init__()
        self.lower_threshold = 0.5
        self.upper_threshold = 0.7
        self.cooldown = 15  # amount of time we don't perform scaling actions
        self.sampling = 5  # sampling rate
        self.dampening = 10  # amount of time that threshold can be breached
        self.upscaling_rate = 2
        self.downscaling_rate = 1

    def __str__(self):
        lt = self.lower_threshold
        ut = self.upper_threshold
        return (
            f"{{ thresholds=({lt:.2f}/{ut:.2f})"
            f" cooldown={self.cooldown} sampling={self.sampling}"
            f" dampening={self.dampening} }}"
        )


class Statistics:
    def __init__(self):
        self.processed = 0
        self.successful = 0
        self.errors = 0
