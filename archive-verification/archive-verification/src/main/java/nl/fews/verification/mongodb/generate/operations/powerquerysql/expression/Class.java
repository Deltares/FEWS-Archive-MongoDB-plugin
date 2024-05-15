package nl.fews.verification.mongodb.generate.operations.powerquerysql.expression;

import nl.fews.verification.mongodb.generate.interfaces.IExecute;
import nl.fews.verification.mongodb.generate.interfaces.IPredecessor;
import nl.fews.verification.mongodb.shared.database.Mongo;
import nl.fews.verification.mongodb.shared.settings.Settings;
import org.bson.Document;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public final class Class implements IExecute, IPredecessor {

	private final String[] predecessors = new String[]{};
	private final String study;

	public Class(String study){
		this.study = study;
	}

	@Override
	public void execute(){
		var name = this.getClass().getSimpleName();
		var database = Settings.get("databaseConnectionString", String.class);
		var table = String.format("%s_%s", study, name);
		var db = Settings.get("verificationDb", String.class);
		var sql = String.format("SELECT * FROM %s.`%s`", db, table);

		var template = String.format("let\n    Source = \"%s\",\n    %s = Odbc.Query(Source, \"%s\")\nin\n    %s", database, name, sql, name);
		Mongo.insertOne("output.PowerQuerySql", new Document("Study", study).append("Name", "ForecastClass").append("Month", "").append("Expression", Arrays.stream(template.replace("\r", "").split("\n")).collect(Collectors.toList())));
		Mongo.insertOne("output.PowerQuerySql", new Document("Study", study).append("Name", "ObservedClass").append("Month", "").append("Expression", Arrays.stream(template.replace("\r", "").split("\n")).collect(Collectors.toList())));
		Mongo.insertOne("output.PowerQuerySql", new Document("Study", study).append("Name", "ContingencyMatrixClass").append("Month", "").append("Expression", Arrays.stream(template.replace("\r", "").split("\n")).collect(Collectors.toList())));
	}

	@Override
	public String[] getPredecessors() {
		return predecessors;
	}
}
