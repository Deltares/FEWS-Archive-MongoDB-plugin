package nl.fews.verification.mongodb.generate.operations.cube;

import nl.fews.verification.mongodb.shared.database.Mongo;
import nl.fews.verification.mongodb.generate.shared.execute.Execute;
import nl.fews.verification.mongodb.generate.shared.graph.Graph;
import nl.fews.verification.mongodb.shared.io.IO;
import nl.fews.verification.mongodb.shared.settings.Settings;
import org.bson.Document;

import java.nio.file.Files;
import java.nio.file.Path;

public class Cube {
	private Cube(){}

	public static void execute(){
		var timingsPath = Path.of(Settings.get("bimPath"), "Timings.json");
		var timings = getTimings(timingsPath);
		IO.deleteFiles(Path.of(Settings.get("bimPath"), ""));
		IO.writeString(timingsPath, timings);

		Mongo.find("Study", new Document("Active", true)).forEach(study ->
			Graph.getDirectedAcyclicGraphGroups(Graph.getDirectedAcyclicGraph(Cube.class, new Object[]{study.getString("Name")})).forEach(Execute::execute));
	}

	private static String getTimings(Path timingsPath){
		try {
			return Files.exists(timingsPath) ? IO.readString(timingsPath) : "";
		}
		catch (Exception ex){
			return "";
		}
	}
}
