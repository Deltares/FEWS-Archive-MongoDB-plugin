package nl.fews.verification.mongodb.generate.operations.view;

import nl.fews.verification.mongodb.shared.database.Mongo;
import org.bson.Document;
import org.yaml.snakeyaml.Yaml;

import java.util.Date;
import java.util.Map;
import java.util.stream.Collectors;

public class Database {
	private Database() {}

	private static final Map<String, String> dataType = Map.of("bson.ObjectId", "objectId", "float64", "double", "int64", "long", "bson.Decimal128", "double");

	public static void insertDrdlYamlToSqlView(String t) {

		var template = Document.parse(new Document(new Yaml().load(t)).toJson());
		var db = template.getList("schema", Document.class).get(0).getString("db");
		var table = template.getList("schema", Document.class).get(0).getList("tables", Document.class).get(0);
		var tableName = String.format("view.dw.%s", table.getString("table"));
		var properties = new Document(table.getList("columns", Document.class).stream().collect(Collectors.toMap(
				c -> c.getString("SqlName"),
				c -> new Document("bsonType", dataType.getOrDefault(c.getString("MongoType"), c.getString("MongoType"))))));

		Mongo.createView(db, tableName, table.getString("collection"), table.getList("pipeline", Document.class));
		Mongo.insertOne(db, "__sql_schemas", new Document().
				append("_id", tableName).
				append("type", "View").
				append("lastUpdated", new Date()).
				append("schema", new Document().
						append("bsonType", "object").
						append("properties", properties)));
	}
}
