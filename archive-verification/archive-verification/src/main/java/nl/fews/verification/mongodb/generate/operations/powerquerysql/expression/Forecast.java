package nl.fews.verification.mongodb.generate.operations.powerquerysql.expression;

import nl.fews.verification.mongodb.generate.interfaces.IExecute;
import nl.fews.verification.mongodb.generate.interfaces.IPredecessor;
import nl.fews.verification.mongodb.shared.database.Mongo;
import nl.fews.verification.mongodb.shared.settings.Settings;
import org.bson.Document;

import java.util.Arrays;

public final class Forecast implements IExecute, IPredecessor {

	private final String[] predecessors = new String[]{};
	private final String study;

	public Forecast(String study){
		this.study = study;
	}

	@Override
	public void execute(){
		var name = this.getClass().getSimpleName();
		var database = Settings.get("databaseConnectionString", String.class);
		var studyDocument = Mongo.findOne("Study", new Document("Name", study));

		var sql = String.format("SELECT forecastId, forecast, ensemble, ensembleMember, `index` FROM %s.`%s`", Settings.get("archiveDb"), String.format("%s_Forecast", study));
		if(!studyDocument.getString("Normal").isEmpty()){
			sql = String.format("%s UNION ALL SELECT 'Normal' AS forecastId, 'Normal' AS forecast, '' AS ensemble, '' AS ensembleMember, 0 AS `index`", sql);
		}

		var template = String.format("let\n    Source = \"%s\",\n    %s = Odbc.Query(Source, \"%s\")\nin\n    %s", database, name, sql, name);
		Mongo.insertOne("output.PowerQuerySql", new Document("Study", study).append("Name", "Forecast").append("Month", "").append("Expression", Arrays.stream(template.replace("\r", "").split("\n")).toList()));
	}

	@Override
	public String[] getPredecessors() {
		return predecessors;
	}
}
