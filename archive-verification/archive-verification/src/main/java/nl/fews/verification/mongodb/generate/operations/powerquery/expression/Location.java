package nl.fews.verification.mongodb.generate.operations.powerquery.expression;

import nl.fews.verification.mongodb.generate.interfaces.IExecute;
import nl.fews.verification.mongodb.generate.interfaces.IPredecessor;
import nl.fews.verification.mongodb.shared.database.Mongo;
import nl.fews.verification.mongodb.shared.settings.Settings;
import org.bson.Document;

import java.util.Arrays;

public final class Location implements IExecute, IPredecessor {

	private final String[] predecessors = new String[]{};
	private final String study;

	public Location(String study){
		this.study = study;
	}

	@Override
	public void execute(){
		var database = Settings.get("databaseConnectionString", String.class);
		var table = String.format("%s_Location", study);
		var db = Settings.get("verificationDb");
		var sql = String.format("SELECT s.location, l.* FROM %s.`%s_Degenerate` s INNER JOIN %s.`%s_Dimension` l ON l.locationId=s.locationId ORDER BY location", db, table, db, table);

		var template = String.join("\n", Mongo.findOne("template.PowerQuery", new Document("Name", "Location")).getList("Template", String.class));
		template = template.replace("{database}", database);
		template = template.replace("{sql}", sql);

		Mongo.insertOne("output.PowerQuery", new Document("Study", study).append("Name", "Location").append("Month", "").append("Expression", Arrays.stream(template.replace("\r", "").split("\n")).toList()));
	}

	@Override
	public String[] getPredecessors() {
		return predecessors;
	}
}
