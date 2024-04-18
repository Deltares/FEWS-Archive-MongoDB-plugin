package nl.fews.verification.mongodb.generate.operations.deploy;

import nl.fews.verification.mongodb.shared.crypto.Crypto;
import nl.fews.verification.mongodb.shared.database.Mongo;
import nl.fews.verification.mongodb.generate.shared.execute.Execute;
import nl.fews.verification.mongodb.generate.shared.graph.Graph;
import nl.fews.verification.mongodb.shared.io.IO;
import nl.fews.verification.mongodb.shared.settings.Settings;
import org.bson.Document;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class Deploy {
	private Deploy(){}

	/**
	 * Executes the deployment process.
	 * This method performs the following steps:
	 * 1. Deletes all files located within the specified bimPath.
	 * 2. Retrieves all studies from the "Verification" collection in the MongoDB database and executes the deployment process for each study.
	 * 3. Restarts the drdlYamlService and checks for successful execution.
	 * 4. Writes the updated Settings.json file to the bimPath directory.
	 *
	 * @throws RuntimeException if an error occurs during the deployment process or if the drdlYamlService restart fails
	 */
	public static void execute(){
		Path timingsPath = Path.of(Settings.get("bimPath"), "Timings.json");
		String timings;
		try {
			timings = Files.exists(timingsPath) ? IO.readString(timingsPath) : "";
		}
		catch (Exception ex){
			timings = "";
		}
		IO.deleteFiles(Path.of(Settings.get("bimPath"), ""));
		IO.writeString(timingsPath, timings);

		Mongo.find("Study", new Document("Active", true)).forEach(study ->
			Graph.getDirectedAcyclicGraphGroups(Graph.getDirectedAcyclicGraph(Deploy.class, new Object[]{study.getString("Name")})).forEach(Execute::execute));

		updateConfig();
		restartService();

		Settings.put("reprocessDays", StreamSupport.stream(Mongo.find("Study", new Document()).spliterator(), false).collect(Collectors.toMap(x -> String.format("Verification_%s", x.getString("Name")), x -> x.getInteger("ReprocessDays"))));
		IO.writeString(Path.of(Settings.get("bimPath"), "Settings.json"), Settings.toJsonString(2));
		Mongo.updateOne("configuration.Settings", new Document(), new Document("$set", new Document("reprocessCubes", "")));
	}

	private static void updateConfig(){
		var drdlYamlConfig = IO.readString(Path.of(Settings.get("drdlYamlConfigPath"), ""));
		if(!drdlYamlConfig.contains(Settings.get("drdlYamlMongoDbUri"))) {
			IO.writeString(Path.of(String.format("%s.bak", Settings.get("drdlYamlConfigPath", String.class)), ""), drdlYamlConfig);
			drdlYamlConfig = drdlYamlConfig.replaceAll("mongodb://.+:\\d+", Settings.get("drdlYamlMongoDbUri", String.class).replace("$", "\\$"));
			drdlYamlConfig = drdlYamlConfig.replaceAll("username:.+", String.format("username: %s", Settings.get("databaseConnectionUsername", String.class).replace("$", "\\$")));
			drdlYamlConfig = drdlYamlConfig.replaceAll("password:.+", String.format("password: %s", Crypto.decrypt(Settings.get("databaseConnectionAesPassword", String.class)).replace("$", "\\$")));
			IO.writeString(Path.of(Settings.get("drdlYamlConfigPath"), ""), drdlYamlConfig);
		}
	}

	private static void restartService(){
		var result = (String)IO.execute(Settings.get("drdlYamlServiceRestart"))[1];

		if (!result.contains("2  START_PENDING") || !result.contains("4  RUNNING"))
			throw new RuntimeException(result);
	}
}
