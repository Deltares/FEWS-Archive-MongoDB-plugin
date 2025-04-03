package nl.fews.verification.mongodb.generate.operations.powerquery.expression;

import nl.fews.verification.mongodb.generate.interfaces.IExecute;
import nl.fews.verification.mongodb.generate.interfaces.IPredecessor;
import nl.fews.verification.mongodb.shared.database.Mongo;
import nl.fews.verification.mongodb.shared.settings.Settings;
import org.bson.Document;

import java.util.Arrays;

public final class Class implements IExecute, IPredecessor {

	private final String[] predecessors = new String[]{};
	private final String study;

	public Class(String study){
		this.study = study;
	}

	@Override
	public void execute(){
		var database = Settings.get("databaseConnectionString", String.class);
		var sql = String.format("SELECT * FROM %s.`%s`", Settings.get("verificationDb"), String.format("%s_Class", study));

		var template = String.join("\n", Mongo.findOne("template.PowerQuery", new Document("Name", "Class")).getList("Template", String.class));
		template = template.replace("{database}", database);
		template = template.replace("{sql}", sql);

		Mongo.insertOne("output.PowerQuery", new Document("Study", study).append("Name", "ForecastClass").append("Month", "").append("Expression", Arrays.stream(template.replace("\r", "").split("\n")).toList()));
		Mongo.insertOne("output.PowerQuery", new Document("Study", study).append("Name", "ObservedClass").append("Month", "").append("Expression", Arrays.stream(template.replace("\r", "").split("\n")).toList()));
		Mongo.insertOne("output.PowerQuery", new Document("Study", study).append("Name", "ContingencyMatrixClass").append("Month", "").append("Expression", Arrays.stream(template.replace("\r", "").split("\n")).toList()));
	}

	@Override
	public String[] getPredecessors() {
		return predecessors;
	}
}
