package nl.fews.verification.mongodb.generate.operations.powerquery.expression;

import nl.fews.verification.mongodb.generate.interfaces.IExecute;
import nl.fews.verification.mongodb.generate.interfaces.IPredecessor;
import nl.fews.verification.mongodb.shared.database.Mongo;
import nl.fews.verification.mongodb.shared.settings.Settings;
import org.bson.Document;

import java.util.Arrays;
import java.util.stream.Collectors;

public final class ForecastTime implements IExecute, IPredecessor {

	private final String[] predecessors = new String[]{};
	private final String study;

	public ForecastTime(String study){
		this.study = study;
	}

	@Override
	public void execute(){
		var database = Settings.get("databaseConnectionString", String.class);
		var sql = String.format("SELECT * FROM %s.`%s`", Settings.get("archiveDb"), String.format("%s_ForecastTime", study));

		var template = String.join("\n", Mongo.findOne("template.PowerQuery", new Document("Name", "ForecastTime")).getList("Template", String.class));
		template = template.replace("{database}", database);
		template = template.replace("{sql}", sql);

		Mongo.insertOne("output.PowerQuery", new Document("Study", study).append("Name", "ForecastTime").append("Month", "").append("Expression", Arrays.stream(template.replace("\r", "").split("\n")).collect(Collectors.toList())));
	}

	@Override
	public String[] getPredecessors() {
		return predecessors;
	}
}
