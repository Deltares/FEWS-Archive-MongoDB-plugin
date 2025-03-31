package nl.fews.verification.mongodb.generate.operations.deploy;

import nl.fews.verification.mongodb.shared.crypto.Crypto;
import nl.fews.verification.mongodb.shared.database.Mongo;
import nl.fews.verification.mongodb.generate.shared.execute.Execute;
import nl.fews.verification.mongodb.generate.shared.graph.Graph;
import nl.fews.verification.mongodb.shared.io.IO;
import nl.fews.verification.mongodb.shared.settings.Settings;
import org.bson.Document;
import org.json.JSONArray;

import java.nio.file.Path;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class Deploy {
	private Deploy(){}

	public static void execute(){
		Mongo.find("Study", new Document("Active", true)).forEach(study ->
			Graph.getDirectedAcyclicGraphGroups(Graph.getDirectedAcyclicGraph(Deploy.class, new Object[]{study.getString("Name")})).forEach(Execute::execute));

		var acquisitionType = Settings.get("acquisitionType", String.class);
		if (acquisitionType.equals("mongodb")){
			updateConfig();
			restartService();
		}
		updateSettings();
	}

	private static void updateSettings(){
		Settings.put("reprocessDays", StreamSupport.stream(Mongo.find("Study", new Document()).spliterator(), false).collect(Collectors.toMap(x -> String.format("Verification_%s", x.getString("Name")), x -> x.getInteger("ReprocessDays"))));
		IO.writeString(Path.of(Settings.get("bimPath"), "Settings.json"), Settings.toJsonString(2));
		Mongo.updateOne("configuration.Settings", new Document("environment",  Settings.get("environment", String.class)), new Document("$set", new Document("reprocessCubes", "")));
	}

	private static void updateConfig(){
		var drdlYamlConfig = IO.readString(Path.of(Settings.get("drdlYamlConfigPath"), ""));
		var matcher = Pattern.compile("mongodb://.+:\\d+").matcher(Settings.get("fewsArchiveDbConnection"));
		var drdlYamlMongoDbUri = matcher.find() ? matcher.group() : "";
		if(!drdlYamlConfig.contains(drdlYamlMongoDbUri)) {
			IO.writeString(Path.of(String.format("%s.bak", Settings.get("drdlYamlConfigPath", String.class)), ""), drdlYamlConfig);
			drdlYamlConfig = drdlYamlConfig.replaceAll("mongodb://.+:\\d+", drdlYamlMongoDbUri.replace("$", "\\$"));
			drdlYamlConfig = drdlYamlConfig.replaceAll("username:.+", String.format("username: %s", Settings.get("fewsArchiveDbUsername", String.class).replace("$", "\\$")));
			drdlYamlConfig = drdlYamlConfig.replaceAll("password:.+", String.format("password: %s", Crypto.decrypt(Settings.get("fewsArchiveDbAesPassword", String.class)).replace("$", "\\$")));
			IO.writeString(Path.of(Settings.get("drdlYamlConfigPath"), ""), drdlYamlConfig);
		}
	}

	private static void restartService(){
		var result = (String)IO.execute(Settings.get("drdlYamlServiceRestart"))[1];
		new JSONArray(Settings.get("drdlYamlServiceRestartSuccess", String.class)).forEach(c -> {
			if (!result.contains(c.toString()))
				throw new RuntimeException(result);
		});
	}
}
