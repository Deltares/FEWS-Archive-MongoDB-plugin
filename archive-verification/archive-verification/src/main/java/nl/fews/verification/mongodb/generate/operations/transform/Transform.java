package nl.fews.verification.mongodb.generate.operations.transform;

import nl.fews.verification.mongodb.shared.database.Mongo;
import nl.fews.verification.mongodb.generate.shared.execute.Execute;
import nl.fews.verification.mongodb.generate.shared.graph.Graph;
import nl.fews.verification.mongodb.shared.io.IO;
import nl.fews.verification.mongodb.shared.settings.Settings;
import org.bson.Document;

import java.nio.file.Path;

public class Transform {
	private Transform(){}

	/**
	 * Executes a series of operations based on the given criteria.
	 * Deletes all files in the specified directory, finds studies in the "Verification" collection,
	 * and executes a series of Transform operations for each study.
	 */
	public static void execute(){
		IO.deleteFiles(Path.of(Settings.get("drdlYamlPath"), ""));
		Mongo.find("Study", new Document("Active", true)).forEach(study ->
			Graph.getDirectedAcyclicGraphGroups(Graph.getDirectedAcyclicGraph(Transform.class, new Object[]{study.getString("Name")})).forEach(Execute::execute));

		String template = String.join("\n", Mongo.findOne("template.DrdlYaml", new Document("Database", Settings.get("verificationDb")).append("Type", "").append("Name", "Info")).getList("Template", String.class));
		IO.writeString(Path.of(Settings.get("drdlYamlPath"),"Info.drdl.yml"), template);
	}
}
