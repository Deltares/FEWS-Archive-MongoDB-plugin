package nl.fews.verification.mongodb.generate.operations.deploy;

import nl.fews.verification.mongodb.shared.database.Mongo;
import nl.fews.verification.mongodb.generate.shared.execute.Execute;
import nl.fews.verification.mongodb.generate.shared.graph.Graph;
import nl.fews.verification.mongodb.shared.io.IO;
import nl.fews.verification.mongodb.shared.settings.Settings;
import org.bson.Document;

import java.nio.file.Path;

public class Deploy {
	private Deploy(){}

	/**
	 * Executes the deployment process.
	 *
	 * This method performs the following steps:
	 * 1. Deletes all files located within the specified bimPath.
	 * 2. Retrieves all studies from the "Verification" collection in the MongoDB database and executes the deployment process for each study.
	 * 3. Restarts the drdlYamlService and checks for successful execution.
	 * 4. Writes the updated Settings.json file to the bimPath directory.
	 *
	 * @throws RuntimeException if an error occurs during the deployment process or if the drdlYamlService restart fails
	 */
	public static void execute(){
		IO.deleteFiles(Path.of(Settings.get("bimPath"), ""));
		Mongo.find("Study", new Document()).forEach(study ->
			Graph.getDirectedAcyclicGraphGroups(Graph.getDirectedAcyclicGraph(Deploy.class, new Object[]{study.getString("Name")})).forEach(Execute::execute));
		var result = (String)IO.execute(Settings.get("drdlYamlServiceRestart"))[1];
		if (!result.contains("2  START_PENDING") || !result.contains("4  RUNNING"))
			throw new RuntimeException(result);
		IO.writeString(Path.of(Settings.get("bimPath"), "Settings.json").toString(), Settings.toJsonString(2));
	}
}
