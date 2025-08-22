import logging


class LogAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        classname = (self.extra or {}).get("classname", "")
        return "%s: %s" % (classname, msg), kwargs
