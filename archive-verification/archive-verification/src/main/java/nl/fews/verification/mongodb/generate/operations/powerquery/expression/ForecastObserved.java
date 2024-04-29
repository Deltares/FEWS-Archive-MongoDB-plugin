package nl.fews.verification.mongodb.generate.operations.powerquery.expression;

import nl.fews.verification.mongodb.generate.interfaces.IExecute;
import nl.fews.verification.mongodb.generate.interfaces.IPredecessor;
import nl.fews.verification.mongodb.shared.database.Mongo;
import nl.fews.verification.mongodb.shared.io.IO;
import nl.fews.verification.mongodb.shared.settings.Settings;
import org.bson.Document;

import java.nio.file.Path;
import java.util.Arrays;

public final class ForecastObserved implements IExecute, IPredecessor {

	private final String[] predecessors = new String[]{};
	private final String study;

	public ForecastObserved(String study){
		this.study = study;
	}

	@Override
	public void execute(){
		var name = this.getClass().getSimpleName();
		var database = Settings.get("databaseConnectionString", String.class);
		var template = String.join("\n", Mongo.findOne("template.PowerQuery", new Document("Name", name)).getList("Template", String.class));
		var db = Settings.get("archiveDb", String.class);

		IO.listFiles(Path.of(Settings.get("drdlYamlPath"), "")).stream().map(s -> s.getFileName().toString()).filter(s -> s.matches(String.format("^%s_\\d{4}-\\d{2}\\.drdl\\.yml$", study))).map(s -> s.replace(String.format("%s_", study), "").replace(".drdl.yml", "")).forEach(month -> {
			var t = template.replace("{database}", database);
			t = t.replace("{db}", db);
			t = t.replace("{study}", study);
			t = t.replace("{month}", month);

			Mongo.insertOne("output.PowerQuery", new Document("Study", study).append("Name", name).append("Month", month).append("Expression", Arrays.stream(t.replace("\r", "").split("\n")).toList()));
		});
	}

	@Override
	public String[] getPredecessors() {
		return predecessors;
	}
}
