package nl.fews.verification.mongodb.generate.operations.powerquery.expression;

import nl.fews.verification.mongodb.generate.interfaces.IExecute;
import nl.fews.verification.mongodb.generate.interfaces.IPredecessor;
import nl.fews.verification.mongodb.shared.database.Mongo;
import nl.fews.verification.mongodb.shared.settings.Settings;
import org.bson.Document;

import java.nio.file.Path;
import java.util.Arrays;

public final class ForecastDate implements IExecute, IPredecessor {

	private final String[] predecessors = new String[]{};
	private final String study;

	public ForecastDate(String study){
		this.study = study;
	}

	@Override
	public void execute(){
		var acquisitionType = Settings.get("acquisitionType", String.class);
		var name = this.getClass().getSimpleName();
		
		String template;
		
		if (acquisitionType.equals("mongodb")){
			var database = Settings.get("databaseConnectionString", String.class);
			var sql = String.format("SELECT * FROM %s.`%s`", Settings.get("archiveDb"), String.format("%s_ForecastDate", study));
	
			template = String.join("\n", Mongo.findOne("template.PowerQuery", new Document("Name", "ForecastDate")).getList("Template", String.class));
			template = template.replace("{database}", database);
			template = template.replace("{sql}", sql);
		}
		else if (acquisitionType.equals("csv")){
			var csvPath = Settings.get("csvPath", String.class);
			template = String.join("\n", Mongo.findOne("template.PowerQuery", new Document("Name", "Csv")).getList("Template", String.class));
			template = template.replace("{rootPath}", Path.of(csvPath, "data").toString());
			template = template.replace("{folderPath}", Path.of(csvPath, "data", study, name) + "\\");
			template = template.replace("{file}", String.format("%s_%s", study, name) + ".csv");
		}
		else {
			throw new IllegalArgumentException(acquisitionType);
		}
		Mongo.insertOne("output.PowerQuery", new Document("Study", study).append("Name", "ForecastDate").append("Month", "").append("Expression", Arrays.stream(template.replace("\r", "").split("\n")).toList()));
	}

	@Override
	public String[] getPredecessors() {
		return predecessors;
	}
}
