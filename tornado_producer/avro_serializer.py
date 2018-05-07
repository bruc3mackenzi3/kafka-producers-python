import io
import struct

from confluent_kafka.avro.cached_schema_registry_client import CachedSchemaRegistryClient
import fastavro._writer


_MAGIC_BYTE = 0

class AvroMessageSerializer(object):

	def __init__(self, schema_subject, schema_registry_url):
		''' Create a new serializer object, which includes the remote-loaded
		schema object specified by schema_subject.

		Note this constructor is not exception safe
		'''

		self.schema_subject = schema_subject
		self.schema_registry_url = schema_registry_url
		self.schema_registry_client = CachedSchemaRegistryClient(url=self.schema_registry_url)

		self._load_schema()

		self.writer = SchemalessAvroRecordWriter(self.avro_schema)

	def _load_schema(self):
		try:
			schema_tuple = self.schema_registry_client.get_latest_schema(subject=self.schema_subject)
		except ValueError as e:
			raise ValueError('Schema subject ' + self.schema_subject + ' not found')
		self.schema_id = schema_tuple[0]
		self.avro_schema = schema_tuple[1].to_json()
		self.schema_version = schema_tuple[2]

	def kafka_avro_encode(self, record):
		with ContextBytesIO() as buf:
			# write the header
			# magic byte
			buf.write(struct.pack('b', _MAGIC_BYTE))
			# write the schema ID in network byte order (big end)
			buf.write(struct.pack('>I', self.schema_id))
			self.writer.write(buf, record)
			return buf.getvalue()


class SchemalessAvroRecordWriter(object):

	def __init__(self, schema):
		self._full_schema = schema
		self._WRITERS = {
				'null': fastavro._writer.write_null,
				'boolean': fastavro._writer.write_boolean,
				'string': fastavro._writer.write_utf8,
				'int': fastavro._writer.write_long,
				'long': fastavro._writer.write_long,
				'float': fastavro._writer.write_float,
				'double': fastavro._writer.write_double,
				'bytes': fastavro._writer.write_bytes,
				'fixed': fastavro._writer.write_fixed,
				'enum': fastavro._writer.write_enum,
				'array': self.write_array,
				'map': self.write_map,
				'union': self.write_union,
				'error_union': self.write_union,
				'record': self.write_record,
				'error': self.write_record,
		}
		self._acquaint_schema(self._full_schema)

	def write(self, fo, record):
		self._write_data(fo, record, self._full_schema)

	def _write_data(self, fo, datum, schema):
		'''Write a datum of data to output stream.
		Paramaters
		----------
		fo: file like
		Output file
		datum: object
			Data to write
		schema: dict
			Schemda to use
		'''

		record_type = fastavro._writer.extract_record_type(schema)
		logical_type = fastavro._writer.extract_logical_type(schema)

		if logical_type:
			prepare = fastavro._writer.LOGICAL_WRITERS[logical_type]
			data = prepare(datum, schema)
			return self._WRITERS[record_type](fo, data, schema)
		else:
			return self._WRITERS[record_type](fo, datum, schema)

	def write_array(self, fo, datum, schema):
		'''Arrays are encoded as a series of blocks.
		Each block consists of a long count value, followed by that many array
		items.  A block with count zero indicates the end of the array.  Each item
		is encoded per the array's item schema.
		If a block's count is negative, then the count is followed immediately by a
		long block size, indicating the number of bytes in the block.  The actual
		count in this case is the absolute value of the count written.  '''

		if len(datum) > 0:
			fastavro._writer.write_long(fo, len(datum))
			dtype = schema['items']
			for item in datum:
				self._write_data(fo, item, dtype)
		fastavro._writer.write_long(fo, 0)

	def write_map(self, fo, datum, schema):
		'''Maps are encoded as a series of blocks.
		Each block consists of a long count value, followed by that many key/value
		pairs.  A block with count zero indicates the end of the map.  Each item is
		encoded per the map's value schema.
		If a block's count is negative, then the count is followed immediately by a
		long block size, indicating the number of bytes in the block. The actual
		count in this case is the absolute value of the count written.'''
		if len(datum) > 0:
			fastavro._writer.write_long(fo, len(datum))
			vtype = schema['values']
			for key, val in fastavro._writer.iteritems(datum):
				fastavro._writer.write_utf8(fo, key)
				self._write_data(fo, val, vtype)
		fastavro._writer.write_long(fo, 0)

	def write_union(self, fo, datum, schema):
		'''A union is encoded by first writing a long value indicating the
		zero-based position within the union of the schema of its value. The value
		is then encoded per the indicated schema within the union.'''

		if isinstance(datum, tuple):
			(name, datum) = datum
			for index, candidate in enumerate(schema):
				if fastavro._writer.extract_record_type(candidate) == 'record':
					if name == candidate['name']:
						break
				else:
					msg = 'provided union type name %s not found in schema %s' \
						   % (name, schema)
					raise ValueError(msg)
		else:
			pytype = type(datum)
			for index, candidate in enumerate(schema):
				if fastavro._writer.validate(datum, candidate):
					break
			else:
				msg = '%r (type %s) do not match %s' % (datum, pytype, schema)
				raise ValueError(msg)

		# write data
		fastavro._writer.write_long(fo, index)
		self._write_data(fo, datum, schema[index])

	def write_record(self, fo, datum, schema):
		'''A record is encoded by encoding the values of its fields in the order
		that they are declared. In other words, a record is encoded as just the
		concatenation of the encodings of its fields.  Field values are encoded per
		their schema.'''
		for field in schema['fields']:
				name = field['name']
				if name not in datum and 'default' not in field and \
								'null' not in field['type']:
					raise ValueError('no value and no default for %s' % name)
				self._write_data(fo, datum.get(
					name, field.get('default')), field['type'])

	def _acquaint_schema(self, schema):
		'''Extract schema into WRITERS repo'''
		repo = self._WRITERS
		fastavro._writer.extract_named_schemas_into_repo(
			schema,
			repo,
			lambda schema: lambda fo, datum, _: self._write_data(fo, datum, schema),
		)
		fastavro._writer.extract_named_schemas_into_repo(
			schema,
			fastavro._writer.SCHEMA_DEFS,
			lambda schema: schema,
		)


class ContextBytesIO(io.BytesIO):

	'''
	Wrapper to allow use of BytesIO via 'with' constructs.
	'''

	def __enter__(self):
		return self

	def __exit__(self, *args):
		self.close()
		return False
