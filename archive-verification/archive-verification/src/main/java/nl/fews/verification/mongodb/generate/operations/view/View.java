package nl.fews.verification.mongodb.generate.operations.view;

import nl.fews.verification.mongodb.generate.shared.execute.Execute;
import nl.fews.verification.mongodb.generate.shared.graph.Graph;
import nl.fews.verification.mongodb.shared.database.Mongo;
import nl.fews.verification.mongodb.shared.settings.Settings;
import org.bson.Document;

import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class View {
	private View(){}

	public static void execute(){
		var environment = Settings.get("environment", String.class);

		if(Settings.get("refreshViews")) {
			StreamSupport.stream(Mongo.listCollections(Settings.get("archiveDb")).filter(new Document("type", "view").append("name", new Document("$regex", String.format("^view\\.verification\\.%s\\.", environment)))).spliterator(), true).parallel().map(c -> c.getString("name")).forEach(c -> Mongo.dropCollection(Settings.get("archiveDb"), c));
			StreamSupport.stream(Mongo.listCollections(Settings.get("verificationDb")).filter(new Document("type", "view").append("name", new Document("$regex", String.format("^view\\.verification\\.%s\\.", environment)))).spliterator(), true).parallel().map(c -> c.getString("name")).forEach(c -> Mongo.dropCollection(Settings.get("verificationDb"), c));
			Settings.put("refreshViews", false);
			Mongo.updateOne("configuration.Settings", new Document("environment", environment), new Document("$set", new Document("refreshViews", false)));
		}

		Mongo.deleteMany("output.View", new Document("Environment", environment).append("State", "new"));

		Mongo.find("Study", new Document("Active", true)).forEach(study ->
			Graph.getDirectedAcyclicGraphGroups(Graph.getDirectedAcyclicGraph(View.class, new Object[]{study.getString("Name")})).forEach(Execute::execute));

		Mongo.deleteMany("output.View", new Document("Environment", environment).append("State", "current"));
		Mongo.updateMany("output.View", new Document("Environment", environment).append("State", "new"), new Document("$set", new Document("State", "current")));

		var archiveCurrent = StreamSupport.stream(Mongo.find("output.View", new Document("Environment", environment).append("Database", Settings.get("archiveDb")).append("State", "current")).spliterator(), false).map(s -> s.getString("View")).collect(Collectors.toSet());
		var archiveExisting = StreamSupport.stream(Mongo.listCollections(Settings.get("archiveDb")).filter(new Document("type", "view").append("name", new Document("$regex", String.format("^view\\.verification\\.%s\\.", environment)))).spliterator(), false).map(s -> s.getString("name")).collect(Collectors.toSet());
		archiveExisting.parallelStream().filter(e -> !archiveCurrent.contains(e)).forEach(v -> Mongo.dropCollection(Settings.get("archiveDb"), v));

		var verificationCurrent = StreamSupport.stream(Mongo.find("output.View", new Document("Environment", environment).append("Database", Settings.get("verificationDb")).append("State", "current")).spliterator(), false).map(s -> s.getString("View")).collect(Collectors.toSet());
		var verificationExisting = StreamSupport.stream(Mongo.listCollections(Settings.get("verificationDb")).filter(new Document("type", "view").append("name", new Document("$regex", String.format("^view\\.verification\\.%s\\.", environment)))).spliterator(), false).map(s -> s.getString("name")).collect(Collectors.toSet());
		verificationExisting.parallelStream().filter(e -> !verificationCurrent.contains(e)).forEach(Mongo::dropCollection);
	}
}