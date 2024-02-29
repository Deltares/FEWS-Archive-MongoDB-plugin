package nl.fews.verification.mongodb.generate.operations.model.expression;

import nl.fews.verification.mongodb.generate.interfaces.IExecute;
import nl.fews.verification.mongodb.generate.interfaces.IPredecessor;
import nl.fews.verification.mongodb.shared.database.Mongo;
import nl.fews.verification.mongodb.shared.settings.Settings;
import org.bson.Document;

import java.util.Arrays;
import java.util.stream.Collectors;

public final class EventDate implements IExecute, IPredecessor {

	private final String[] predecessors = new String[]{};
	private final String study;

	public EventDate(String study){
		this.study = study;
	}

	/**
	 * The `execute` method is designed to generate an `output.PowerQuery` document for "EventDate", retrieving necessary data from MongoDB, performing the corresponding manipulations on the data, and then storing the final outcome back into MongoDB.
	 *
	 * The overarching workflow of the `execute` method can be outlined as:
	 *
	 * 1. Retrieves a study document from the `Verification` database in MongoDB, specifically from the `Study` collection that aligns with a given `study`.
	 * 2. Fetches a power query template from the `template.PowerQuery` collection in the `Verification` database that matches "EventDate". The template strings are combined into a single string for further processing.
	 * 3. Gets the database connection string from the application settings.
	 * 4. Iterates over each forecast in the `studyDocument`. For each forecast, it retrieves the corresponding document from the `Forecast` collection in the `Verification` database and creates an SQL query string based on the forecast and its filters. The resulting string collates all query strings.
	 * 5. Replaces placeholders in the template string (i.e., "{database}" and "{queries}") with the actual database connection string and the collated SQL query string.
	 * 6. Lastly, it inserts the final template string into the `Verification` database in the `output.PowerQuery` collection as a new document.
	 *
	 * As a point to note, this method largely leverages MongoDB for fetching and storing data, and also for string handling and processing in conjunction with Java's Stream API.
	 */
	@Override
	public void execute(){
		Document studyDocument = Mongo.findOne("Study", new Document("Name", study));
		String template = String.join("\n", Mongo.findOne("template.PowerQuery", new Document("Name", "EventDate")).getList("Template", String.class));
		String database = Settings.get("databaseConnectionString");
		String queries = studyDocument.getList("Forecasts", String.class).stream().map(s -> {
			Document forecastDocument = Mongo.findOne("Forecast", new Document("Name", s));
			return forecastDocument.getList("Filters", Document.class).stream().map(
				f -> String.format("        Odbc.Query(Source, \"SELECT * FROM %s.%s\")", Settings.get("archiveDb"), String.format("%s_%s_EventDate_%s", study, forecastDocument.getString("ForecastName"), f.getString("FilterName")))).collect(Collectors.joining(",\n"));
		}).collect(Collectors.joining(",\n"));

		template = template.replace("{database}", database);
		template = template.replace("{queries}", queries);

		Mongo.insertOne("output.PowerQuery", new Document("Study", study).append("Name", "EventDate").append("Month", "").append("Expression", Arrays.stream(template.replace("\r", "").split("\n")).toList()));
	}

	/**
	 * Retrieves the predecessors of the current object.
	 *
	 * @return An array of strings representing the predecessors.
	 */
	@Override
	public String[] getPredecessors() {
		return predecessors;
	}
}
