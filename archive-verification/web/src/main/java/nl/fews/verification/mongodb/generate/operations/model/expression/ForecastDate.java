package nl.fews.verification.mongodb.generate.operations.model.expression;

import nl.fews.verification.mongodb.generate.interfaces.IExecute;
import nl.fews.verification.mongodb.generate.interfaces.IPredecessor;
import nl.fews.verification.mongodb.shared.database.Mongo;
import nl.fews.verification.mongodb.shared.settings.Settings;
import org.bson.Document;

import java.util.Arrays;
import java.util.stream.Collectors;

public final class ForecastDate implements IExecute, IPredecessor {

	private final String[] predecessors = new String[]{};
	private final String study;

	public ForecastDate(String study){
		this.study = study;
	}

	/**
	 * The `execute` method is responsible for formulating and executing SQL queries to MongoDB,
	 * performing manipulations on the returned results, and inserting the final result back
	 * into the MongoDB in the `output.PowerQuery` collection.
	 *
	 * The core workflow of the `execute` method is as follows:
	 *
	 * 1. It starts by querying the `Verification` database within MongoDB, to retrieve a
	 *    document from the `Study` collection that matches a field `Name` with a given `study`.
	 * 2. It retrieves a template string by querying the `Verification` database within MongoDB,
	 *    retrieving a document from the `template.PowerQuery` collection that matches a field
	 *    `Name` with the string "ForecastDate", and joins all the items in the `Template` field
	 *    of the returned document with newline characters.
	 * 3. It gets a database connection string from the settings.
	 * 4. It then begins a series of transformations on granularity of `Forecasts` (from `studyDocument`)
	 *    Each named forecast goes through a more refined transformation in a nested loop where every filter of a forecast is used to format a SQL query string.
	 *    All these queries are combined to form a single queries string.
	 * 5. The template string gotten earlier is updated with the replaced placeholders i.e. `{database}` and `{queries}`.
	 * 6. This query string is then inserted back into the `Verification` database in the `output.PowerQuery` collection.
	 *    The new document to be inserted is built up with `Study`, `Name`, `Month` and `Expression` fields.
	 *
	 * This method makes heavy use of String formatting for the SQL query and the Java 8 Streams API for data
	 * transformation. The transformed results are finally written back to MongoDB.
	 */
	@Override
	public void execute(){
		Document studyDocument = Mongo.findOne("Study", new Document("Name", study));
		String template = String.join("\n", Mongo.findOne("template.PowerQuery", new Document("Name", "ForecastDate")).getList("Template", String.class));
		String database = Settings.get("databaseConnectionString");
		String queries = studyDocument.getList("Forecasts", String.class).stream().map(s -> {
			Document forecastDocument = Mongo.findOne("Forecast", new Document("Name", s));
			return forecastDocument.getList("Filters", Document.class).stream().map(
				f -> String.format("        Odbc.Query(Source, \"SELECT * FROM %s.%s\")", Settings.get("archiveDb"), String.format("%s_%s_ForecastDate_%s", study, forecastDocument.getString("ForecastName"), f.getString("FilterName")))).collect(Collectors.joining(",\n"));
		}).collect(Collectors.joining(",\n"));

		template = template.replace("{database}", database);
		template = template.replace("{queries}", queries);

		Mongo.insertOne("output.PowerQuery", new Document("Study", study).append("Name", "ForecastDate").append("Month", "").append("Expression", Arrays.stream(template.replace("\r", "").split("\n")).toList()));
	}

	/**
	 * Retrieves the predecessors of the current object.
	 *
	 * @return An array of strings representing the predecessors of the current object.
	 */
	@Override
	public String[] getPredecessors() {
		return predecessors;
	}
}
