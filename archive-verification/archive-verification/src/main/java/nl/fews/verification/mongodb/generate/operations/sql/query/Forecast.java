package nl.fews.verification.mongodb.generate.operations.sql.query;

import nl.fews.verification.mongodb.generate.interfaces.IExecute;
import nl.fews.verification.mongodb.generate.interfaces.IPredecessor;
import nl.fews.verification.mongodb.shared.database.Mongo;
import nl.fews.verification.mongodb.shared.settings.Settings;
import org.bson.Document;

public final class Forecast implements IExecute, IPredecessor {

	private final String[] predecessors = new String[]{};
	private final String study;

	public Forecast(String study){
		this.study = study;
	}

	@Override
	public void execute(){
		var studyDocument = Mongo.findOne("Study", new Document("Name", study));
		var sql = String.format("SELECT forecastId, forecast, ensemble, ensembleMember, `index` FROM %s.`%s`", Settings.get("archiveDb"), String.format("%s_Forecast", study));
		if(!studyDocument.getString("Normal").isEmpty()){
			sql = String.format("%s UNION ALL SELECT 'Normal' AS forecastId, 'Normal' AS forecast, '' AS ensemble, '' AS ensembleMember, 0 AS `index`", sql);
		}
		Mongo.insertOne("output.Sql", new Document("Study", study).append("Name", "Forecast").append("Month", "").append("Query", sql));
	}

	@Override
	public String[] getPredecessors() {
		return predecessors;
	}
}
