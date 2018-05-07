import simplejson as json


class ProducerConfig:
	__instance = None

	@staticmethod
	def get_instance(config_filename):
		if not ProducerConfig.__instance:
			ProducerConfig(config_filename)
		return ProducerConfig.__instance

	def __init__(self, config_filename):
		if ProducerConfig.__instance:
			raise Exception("Singleton class already initialized")

		else:
			self.__config = json.load(open(config_filename))
			for name, value in self.__config.iteritems():
				setattr(self, name, value)

			ProducerConfig.__instance = self
