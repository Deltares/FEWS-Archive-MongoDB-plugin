package nl.fews.verification.mongodb.generate.operations.powerquerysql.expression;

import nl.fews.verification.mongodb.generate.interfaces.IExecute;
import nl.fews.verification.mongodb.generate.interfaces.IPredecessor;
import nl.fews.verification.mongodb.shared.database.Mongo;
import nl.fews.verification.mongodb.shared.settings.Settings;
import org.bson.Document;

import java.util.Arrays;

public final class LeadTime implements IExecute, IPredecessor {

	private final String[] predecessors = new String[]{};
	private final String study;

	public LeadTime(String study){
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
		Mongo.insertOne("output.PowerQuery", new Document("Study", study).append("Name", "LeadTime").append("Month", "").append("Expression", Arrays.stream(template.replace("\r", "").split("\n")).toList()));
	}

	@Override
	public String[] getPredecessors() {
		return predecessors;
	}
}
