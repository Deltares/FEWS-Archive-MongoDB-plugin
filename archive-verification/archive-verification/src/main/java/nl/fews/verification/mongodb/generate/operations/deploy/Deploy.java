package nl.fews.verification.mongodb.generate.operations.deploy;

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

		updateConfig();
		//restartService();
		updateSettings();
	}

	private static void updateSettings(){
		Settings.put("reprocessDays", StreamSupport.stream(Mongo.find("Study", new Document()).spliterator(), false).collect(Collectors.toMap(x -> String.format("Verification_%s", x.getString("Name")), x -> x.getInteger("ReprocessDays"))));
		IO.writeString(Path.of(Settings.get("bimPath"), "Settings.json"), Settings.toJsonString(2));
		Mongo.updateOne("configuration.Settings", new Document("environment",  Settings.get("environment", String.class)), new Document("$set", new Document("reprocessCubes", "")));
	}

	private static void updateConfig(){
		var drdlYamlConfig = IO.readString(Path.of(Settings.get("drdlYamlConfigPath"), ""));
		var matcher = Pattern.compile("mongodb://(.+):(.+)@(.+):(\\d+)/").matcher(Settings.get("mongoVerificationDbConnection"));
		if(matcher.find()){
			var user = matcher.group(1);
			var pass = matcher.group(2);
			var server = matcher.group(3);
			var port = matcher.group(4);
			var uri = String.format("mongodb://%s:%s/", server, port);
			if(!drdlYamlConfig.contains(uri)) {
				IO.writeString(Path.of(String.format("%s.bak", Settings.get("drdlYamlConfigPath", String.class)), ""), drdlYamlConfig);
				drdlYamlConfig = drdlYamlConfig.replaceAll("mongodb://.+/", uri.replace("$", "\\$"));
				drdlYamlConfig = drdlYamlConfig.replaceAll("username:.+", String.format("username: %s", user.replace("$", "\\$")));
				drdlYamlConfig = drdlYamlConfig.replaceAll("password:.+", String.format("password: %s", pass.replace("$", "\\$")));
				IO.writeString(Path.of(Settings.get("drdlYamlConfigPath"), ""), drdlYamlConfig);
			}
		}
		else {
			throw new IllegalArgumentException("mongoVerificationDbConnection should match pattern \"mongodb://(.+):(.+)@(.+):(\\d+)/\"");
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
