class WorkItemException(Exception):
    pass


class RouteToErrorQueue(WorkItemException):
    def __init__(self, msg):
        super().__init__(msg)


class StopProcessing(WorkItemException):
    def __init__(self, msg):
        super().__init__(msg)


class LastWorkItem:
    pass

