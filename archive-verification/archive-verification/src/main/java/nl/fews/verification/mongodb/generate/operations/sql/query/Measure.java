package nl.fews.verification.mongodb.generate.operations.sql.query;

import nl.fews.verification.mongodb.generate.interfaces.IExecute;
import nl.fews.verification.mongodb.generate.interfaces.IPredecessor;
import nl.fews.verification.mongodb.shared.database.Mongo;
import nl.fews.verification.mongodb.shared.settings.Settings;
import org.bson.Document;

public final class Measure implements IExecute, IPredecessor {

	private final String[] predecessors = new String[]{};
	private final String study;

	public Measure(String study){
		this.study = study;
	}

	@Override
	public void execute(){
		var sql = String.format("SELECT * FROM %s.`%s`", Settings.get("verificationDb"), String.format("%s_Measure", study));
		Mongo.insertOne("output.Sql", new Document("Study", study).append("Name", "Measure").append("Month", "").append("Query", sql));
	}

	@Override
	public String[] getPredecessors() {
		return predecessors;
	}
}