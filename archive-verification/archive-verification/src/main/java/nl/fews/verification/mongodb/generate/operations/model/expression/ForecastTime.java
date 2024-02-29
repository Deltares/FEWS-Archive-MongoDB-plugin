package nl.fews.verification.mongodb.generate.operations.model.expression;

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

	/**
	 * Executes the necessary operations to generate and insert a template into the MongoDB collection.
	 * This method retrieves information from the 'Verification' database, constructs a template, and inserts it into the 'output.PowerQuery' collection.
	 * The template is generated based on the provided study and forecasts information.
	 */
	@Override
	public void execute(){
		Document studyDocument = Mongo.findOne("Study", new Document("Name", study));
		String template = String.join("\n", Mongo.findOne("template.PowerQuery", new Document("Name", "ForecastTime")).getList("Template", String.class));
		String database = Settings.get("databaseConnectionString");
		String queries = studyDocument.getList("Forecasts", String.class).stream().map(s -> {
			Document forecastDocument = Mongo.findOne("Forecast", new Document("Name", s));
			return forecastDocument.getList("Filters", Document.class).stream().map(
				f -> String.format("        Odbc.Query(Source, \"SELECT * FROM %s.%s\")", Settings.get("archiveDb"), String.format("%s_%s_ForecastTime_%s", study, forecastDocument.getString("ForecastName"), f.getString("FilterName")))).collect(Collectors.joining(",\n"));
		}).collect(Collectors.joining(",\n"));

		template = template.replace("{database}", database);
		template = template.replace("{queries}", queries);

		Mongo.insertOne("output.PowerQuery", new Document("Study", study).append("Name", "ForecastTime").append("Month", "").append("Expression", Arrays.stream(template.replace("\r", "").split("\n")).toList()));
	}

	/**
	 * Returns an array of strings representing the predecessors of the object.
	 *
	 * @return an array of strings
	 */
	@Override
	public String[] getPredecessors() {
		return predecessors;
	}
}
