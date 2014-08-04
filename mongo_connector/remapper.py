import re

def clean_path(dirty):
	"""Convert a string of python subscript notation or mongo dot-notation to a
		list of strings.
	"""
	# handle python dictionary subscription style, e.g. `"['key1']['key2']"`:
	if re.match(r'^\[', dirty):
		return re.split(r'\'\]\[\'', re.sub(r'^\[\'|\'\]$', '', dirty))
	# handle mongo op dot-notation style, e.g. `"key1.key2"`:
	return dirty.split('.')


def get_at(doc, path, create_anyway=False):
	"""Get the value, if any, of the document at the given path, optionally
		mutating the document to create nested dictionaries as necessary.
	"""
	node = doc
	last = len(path) - 1
	if last == 0:
		return doc.get(path[0])
	for index, edge in enumerate(path):
		if edge in node:
			node = node[edge]
		elif index == last or not create_anyway:
			# the key doesn't exist, and this is the end of the path:
			return None
		else:
			# create anyway will create any missing nodes:
				node = node[edge] = {}
	return node


def set_at(doc, path, value):
	"""Set the value of the document at the given path."""
	node = get_at(doc, path[:-1], create_anyway=True)
	node[path[-1]] = value


def put_at(doc, path, value, append=False):
	"""Set or append the given value to the document at the given path"""
	if append:
		get_at(doc, path).append(value)
	else:
		set_at(doc, path, value)


def filter_value(value, expr):
	"""Evaluate the given expression in the context of the given value."""
	if expr == "":
		return True
	try:
		return eval(re.sub(r'\$_', 'value', expr))
	except Exception as e:
		logging.warn("""
				Error raised from expression: {filter} with value {value}
				""".format(**locals()))
		logging.warn(e)
		# return false to prevent potentially sensitive data from being synced:
		return False


class Remapper:

	def __init__(remap_json):
		self.attributes_remap = remap_json['remap']
		self.attributes_filter = remap_json['filter']

	def remap(doc):
		return self.apply_filter(self.apply_remap(doc))

	def apply_remap(self, doc):
		"""Copy the values of user-defined fields from the source document to
			user-defined fields in a new target document, then return the
			targetdocument.
		"""
		if not self.attributes_remap:
			return doc
		remapped_doc = {}
		for raw_source_key, raw_target_key in self.attributes_remap.items():
			# clean the keys, making a list from possible notations:
			source_key = clean_path(raw_source_key)
			target_key = clean_path(raw_target_key)

			# get the value from the source doc:
			value = get_at(doc, source_key)

			# special case for "_ts" field:
			if source_key == ['_ts'] and target_key == ["*ts*"]:
				value = value if value else str(unix_time_millis())

			set_at(remapped_doc, target_key, value)
		return remapped_doc

	def apply_filter(self, doc):
		"""Recursively copy the values of user-defined fields from the source
			document to a new target document by testing each value against a
			corresponding user-defined expression. If the expression returns
			true for a given value, copy that value to the corresponding field
			in the target document. If the special `*all*` filter is used for
			a given document and an adjacent field's expression returns false
			for a given value, remove the document containing that field from
			its parent in the tree of the target document.
		"""
		filter = self.attributes_filter
		if not filter:
			# alway return a new object:
			return (copy.deepcopy(doc), True)
		filtered_doc = {}
		all_or_nothing = '*all*' in filter
		for raw_key, expr in filter.iteritems():
			if raw_key == '*all*':
				continue
			key = clean_path(raw_key)
			values = get_at(doc, key)
			state = True
			if type(values) == list:
				append = True
				set_at(filtered_doc, key, [])
			else:
				append = False
				values = [values]
			for value in values:
				if isinstance(value, dict):
					sub, sub_state = self.apply_filter(value, filter[raw_key])
					if sub_state:
						put_at(filtered_doc, key, serialize(sub), append)
					elif all_or_nothing:
						node = get_at(filtered_doc, key[:-1])
						del node[key[-1]]
						return filtered_doc, False
				elif filter_value(value, filter[raw_key]):
					put_at(filtered_doc, key, serialize(value), append)
				elif all_or_nothing:
					return filtered_doc, False
				else:
					state = False
		return (filtered_doc, state)
