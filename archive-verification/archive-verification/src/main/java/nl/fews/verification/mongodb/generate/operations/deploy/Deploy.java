package nl.fews.verification.mongodb.generate.operations.deploy;

import nl.fews.verification.mongodb.shared.database.Mongo;
import nl.fews.verification.mongodb.generate.shared.execute.Execute;
import nl.fews.verification.mongodb.generate.shared.graph.Graph;
import nl.fews.verification.mongodb.shared.io.IO;
import nl.fews.verification.mongodb.shared.settings.Settings;
import org.bson.Document;

import java.nio.file.Path;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class Deploy {
	private Deploy(){}

	public static void execute(){
		Mongo.find("Study", new Document("Active", true)).forEach(study ->
			Graph.getDirectedAcyclicGraphGroups(Graph.getDirectedAcyclicGraph(Deploy.class, new Object[]{study.getString("Name")})).forEach(Execute::execute));
		updateSettings();
	}

	private static void updateSettings(){
		Settings.put("reprocessDays", StreamSupport.stream(Mongo.find("Study", new Document()).spliterator(), false).collect(Collectors.toMap(x -> String.format("Verification_%s", x.getString("Name")), x -> x.getInteger("ReprocessDays"))));
		IO.writeString(Path.of(Settings.get("bimPath"), "Settings.json"), Settings.toJsonString(2));
		Mongo.updateOne("configuration.Settings", new Document("environment",  Settings.get("environment", String.class)), new Document("$set", new Document("reprocessCubes", "")));
	}
}
