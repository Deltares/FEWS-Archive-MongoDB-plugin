package nl.fews.verification.mongodb.generate.operations.powerquerysql.expression;

import nl.fews.verification.mongodb.generate.interfaces.IExecute;
import nl.fews.verification.mongodb.generate.interfaces.IPredecessor;
import nl.fews.verification.mongodb.shared.database.Mongo;
import nl.fews.verification.mongodb.shared.io.IO;
import nl.fews.verification.mongodb.shared.settings.Settings;
import org.bson.Document;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.stream.Collectors;

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
		var db = Settings.get("archiveDb", String.class);

		IO.listFiles(Path.of(Settings.get("drdlYamlPath"), "")).stream().map(s -> s.getFileName().toString()).filter(s -> s.matches(String.format("^%s_\\d{4}-\\d{2}\\.drdl\\.yml$", study))).map(s -> s.replace(".drdl.yml", "")).forEach(table -> {
			var sql = String.format("SELECT * FROM %s.`%s`", db, table);
			var template = String.format("let\n    Source = \"%s\",\n    %s = Odbc.Query(Source, \"%s\")\nin\n    %s", database, name, sql, name);
			Mongo.insertOne("output.PowerQuerySql", new Document("Study", study).append("Name", "ForecastObserved").append("Month", table.replace(String.format("%s_", study), "")).append("Expression", Arrays.stream(template.replace("\r", "").split("\n")).collect(Collectors.toList())));
		});
	}

	@Override
	public String[] getPredecessors() {
		return predecessors;
	}
}
