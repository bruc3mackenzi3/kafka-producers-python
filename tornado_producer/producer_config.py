import simplejson as json


class ProducerConfig:
    def __init__(self, config_filename):
        self.__config = json.load(open(config_filename))
        for name, value in self.__config.iteritems():
            setattr(self, name, value)
