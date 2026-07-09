# Copyright (c) 2026 INFISYS / RTI International / TVA

import yaml
import json
from datetime import datetime

header = f'/*\n * Copyright (c) {datetime.now().year} INFISYS / RTI International / TVA\n */\n'
dt = {
	'bson.ObjectId': 'objectId',
	'float64': 'double',
	'bson.Decimal128': 'double',
}


def write_schema(db):
	schemas = []
	for t in db['tables']:
		schemas.append({
			"_id": f"view.{t['table']}",
			'type': 'View',
			'lastUpdated': 'new Date()',
			'schema': {
				'bsonType': 'object',
				'properties': {c['SqlName']: {'bsonType': dt.get(c['MongoType'], c['MongoType'])} for c in t['columns']}
			}
		})
	output = [
		header,
		"db.getCollection('__sql_schemas').drop()",
		"db.getCollection('__sql_schemas').insertMany(",
		json.dumps(schemas, indent=2, default=str).replace('"new Date()"', 'new Date()'),
		');']

	with open(f'{db["db"].lower()}.schema.js', 'w') as f:
		f.write('\n'.join(output))


def write_view(t):
	views = []
	for t in db['tables']:
		project = {"$project": {"_id": 0, **{c['SqlName']: f'${c["Name"]}' for c in t['columns']}}}
		pipeline = [*t.get('pipeline', []), project]
		views.append('\n'.join([
			f'db.getCollection("view.{t["table"]}").drop();',
			f'db.createView("view.{t["table"]}", "{t["collection"]}", ',
			json.dumps(pipeline, indent=2, default=str),
			');'
		]))
	output = [header, *views]
	with open(f'{db["db"].lower()}.views.js', 'w') as f:
		f.write('\n'.join(output))


with open('fews.drdl.yml', 'r') as r:
	yml = yaml.safe_load(r)

for db in yml['schema']:
	write_schema(db)
	write_view(db)
