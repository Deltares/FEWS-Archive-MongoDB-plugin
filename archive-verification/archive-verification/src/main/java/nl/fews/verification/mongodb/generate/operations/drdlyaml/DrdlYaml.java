package nl.fews.verification.mongodb.generate.operations.drdlyaml;

import nl.fews.verification.mongodb.shared.database.Mongo;
import nl.fews.verification.mongodb.generate.shared.execute.Execute;
import nl.fews.verification.mongodb.generate.shared.graph.Graph;
import nl.fews.verification.mongodb.shared.io.IO;
import nl.fews.verification.mongodb.shared.settings.Settings;
import org.bson.Document;

import java.nio.file.Files;
import java.nio.file.Path;

public class DrdlYaml {
	private DrdlYaml(){}

	public static void execute(){
		var acquisitionType = Settings.get("acquisitionType", String.class);

		if (acquisitionType.equals("mongodb")) {
			var drdlYamlPath = Path.of(Settings.get("drdlYamlPath", String.class), "");
			if (!Files.exists(drdlYamlPath))
				IO.createDirectories(drdlYamlPath);
			IO.deleteFiles(drdlYamlPath);
		}
		else if (acquisitionType.equals("csv")) {
			Mongo.deleteMany("output.DrdlYaml", new Document());
		}
		
		Mongo.find("Study", new Document("Active", true)).forEach(study ->
			Graph.getDirectedAcyclicGraphGroups(Graph.getDirectedAcyclicGraph(DrdlYaml.class, new Object[]{study.getString("Name")})).forEach(Execute::execute));

		if (acquisitionType.equals("mongodb")) {
			var template = String.join("\n", Mongo.findOne("template.DrdlYaml", new Document("Type", "").append("Name", "Info")).getList("Template", String.class));
			IO.writeString(Path.of(Settings.get("drdlYamlPath"), "Info.drdl.yml"), template);
		}
	}
}
