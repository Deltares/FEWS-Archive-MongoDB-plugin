package nl.fews.verification.mongodb.generate.operations.sql.query;

import nl.fews.verification.mongodb.generate.interfaces.IExecute;
import nl.fews.verification.mongodb.generate.interfaces.IPredecessor;
import nl.fews.verification.mongodb.shared.database.Mongo;
import nl.fews.verification.mongodb.shared.io.IO;
import nl.fews.verification.mongodb.shared.settings.Settings;
import org.bson.Document;

import java.nio.file.Path;

public final class ForecastObserved implements IExecute, IPredecessor {

	private final String[] predecessors = new String[]{};
	private final String study;

	public ForecastObserved(String study){
		this.study = study;
	}

	@Override
	public void execute(){
		IO.listFiles(Path.of(Settings.get("drdlYamlPath"), "")).stream().map(s -> s.getFileName().toString()).filter(s -> s.matches(String.format("^%s_\\d{4}-\\d{2}\\.drdl\\.yml$", study))).map(s -> s.replace(".drdl.yml", "")).forEach(s -> {
			var sql = String.format("SELECT * FROM %s.`%s`", Settings.get("archiveDb"), s);
			Mongo.insertOne("output.Sql", new Document("Study", study).append("Name", "ForecastObserved").append("Month", s.replace(String.format("%s_", study), "")).append("Query", sql));
		});
	}

	@Override
	public String[] getPredecessors() {
		return predecessors;
	}
}
