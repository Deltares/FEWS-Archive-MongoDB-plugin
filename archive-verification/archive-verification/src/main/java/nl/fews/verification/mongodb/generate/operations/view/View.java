package nl.fews.verification.mongodb.generate.operations.view;

import nl.fews.verification.mongodb.generate.shared.execute.Execute;
import nl.fews.verification.mongodb.generate.shared.graph.Graph;
import nl.fews.verification.mongodb.shared.database.Mongo;
import nl.fews.verification.mongodb.shared.io.IO;
import nl.fews.verification.mongodb.shared.settings.Settings;
import org.bson.Document;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;

public class View {
	private View(){}

	public static void execute(){
		var drdlYamlPath = Path.of(Settings.get("drdlYamlPath", String.class), "");
		if (!Files.exists(drdlYamlPath))
			IO.createDirectories(drdlYamlPath);
		IO.deleteFiles(drdlYamlPath);

		Mongo.deleteMany("__sql_schemas", new Document("_id", new Document("$regex", "^view\\.dw\\.")));
		for(var collection: Mongo.listCollections().filter(new Document("name", new Document("$regex", "^view\\.dw\\."))).map(c -> c.getString("name")).into(new ArrayList<>()))
			Mongo.dropCollection(collection);

		Mongo.deleteMany("local", "__sql_schemas", new Document("_id", new Document("$regex", "^view\\.dw\\.")));
		for(var collection: Mongo.listCollections("local").filter(new Document("name", new Document("$regex", "^view\\.dw\\."))).map(c -> c.getString("name")).into(new ArrayList<>()))
			Mongo.dropCollection("local", collection);

		Mongo.find("Study", new Document("Active", true)).forEach(study ->
			Graph.getDirectedAcyclicGraphGroups(Graph.getDirectedAcyclicGraph(View.class, new Object[]{study.getString("Name")})).forEach(Execute::execute));

		var template = String.join("\n", Mongo.findOne("template.DrdlYaml", new Document("Type", "").append("Name", "Info")).getList("Template", String.class));
		Database.insertDrdlYamlToSqlView(template);
	}
}
