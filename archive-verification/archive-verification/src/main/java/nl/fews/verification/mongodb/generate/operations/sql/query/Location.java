package nl.fews.verification.mongodb.generate.operations.sql.query;

import nl.fews.verification.mongodb.generate.interfaces.IExecute;
import nl.fews.verification.mongodb.generate.interfaces.IPredecessor;
import nl.fews.verification.mongodb.shared.database.Mongo;
import nl.fews.verification.mongodb.shared.settings.Settings;
import org.bson.Document;

public final class Location implements IExecute, IPredecessor {

	private final String[] predecessors = new String[]{};
	private final String study;

	public Location(String study){
		this.study = study;
	}

	@Override
	public void execute(){
		var table = String.format("%s_Location", study);
		var sql = String.format("SELECT s.location, l.* FROM %s.`%s` s INNER JOIN %s.`%s` l ON l.locationId=s.locationId ORDER BY location", Settings.get("archiveDb"), table, Settings.get("verificationDb"), table);
		Mongo.insertOne("output.Sql", new Document("Study", study).append("Name", "Location").append("Month", "").append("Query", sql));
	}

	@Override
	public String[] getPredecessors() {
		return predecessors;
	}
}
