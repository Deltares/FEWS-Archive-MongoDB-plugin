package nl.fews.verification.mongodb.generate.operations.model.expression;

import nl.fews.verification.mongodb.generate.interfaces.IExecute;
import nl.fews.verification.mongodb.generate.interfaces.IPredecessor;
import nl.fews.verification.mongodb.shared.database.Mongo;
import nl.fews.verification.mongodb.shared.settings.Settings;
import org.bson.Document;

import java.util.Arrays;
import java.util.stream.Collectors;

public final class Forecast implements IExecute, IPredecessor {

	private final String[] predecessors = new String[]{};
	private final String study;

	public Forecast(String study){
		this.study = study;
	}

	/**
	 * The `execute` method serves to generate an `output.PowerQuery` for forecasts using a template
	 * retrieved from a MongoDB database, filling it with the transformation of different forecasted
	 * scenarios available in the `Forecasts` of a `Study`, and storing the finalised template back into MongoDB.
	 *
	 * The key steps followed by the method are:
	 *
	 * 1. Retrieve a specified study document from the `Verification` database in MongoDB.
	 * 2. Retrieve a PowerQuery template from the `Verification` database in MongoDB, which is then
	 *    processed to a string format used later for results generation.
	 * 3. Retrieve the database connection string from the application settings.
	 * 4. For every forecast in the retrieved `Study` document, get its filters. For every filter,
	 *    an SQL command is generated and all such commands for all forecasts and filters are joined
	 *    to form a master command string.
	 * 5. In the template string retrieved earlier, placeholders for the Database and Queries are replaced
	 *    with the Database Connection String and the master command string respectively.
	 * 6. The finalised query (template updated with replacements in step 5) is inserted back into the
	 *    Verification database in the `output.PowerQuery` collection.
	 *
	 * Note: This method relies heavily on the MongoDB querying capability, pulling, preprocessing,
	 * and pushing data in and out of the MongoDB. The core computational work is string manipulation and
	 * working with Java Streams.
	 */
	@Override
	public void execute(){
		Document studyDocument = Mongo.findOne("Study", new Document("Name", study));
		String template = String.join("\n", Mongo.findOne("template.PowerQuery", new Document("Name", "Forecast")).getList("Template", String.class));
		String database = Settings.get("databaseConnectionString");
		String queries = studyDocument.getList("Forecasts", String.class).stream().map(s -> {
			Document forecastDocument = Mongo.findOne("Forecast", new Document("Name", s));
			return forecastDocument.getList("Filters", Document.class).stream().map(
				f -> String.format("        Odbc.Query(Source, \"SELECT forecastId, forecast, ensemble, ensembleMember FROM %s.%s\")", Settings.get("archiveDb"), String.format("%s_%s_Forecast_%s", study, forecastDocument.getString("ForecastName"), f.getString("FilterName")))).collect(Collectors.joining(",\n"));
		}).collect(Collectors.joining(",\n"));
		if(!studyDocument.getString("Normal").isEmpty()){
			queries = String.format("%s,\n        Odbc.Query(Source, \"SELECT 'Normal' AS forecastId, 'Normal' AS forecast, '' AS ensemble, '' AS ensembleMember\")", queries);
		}

		template = template.replace("{database}", database);
		template = template.replace("{queries}", queries);

		Mongo.insertOne("output.PowerQuery", new Document("Study", study).append("Name", "Forecast").append("Month", "").append("Expression", Arrays.stream(template.replace("\r", "").split("\n")).collect(Collectors.toList())));
	}

	/**
	 * Retrieves an array of predecessors for the current object.
	 *
	 * @return An array of strings representing the names of the predecessors.
	 */
	@Override
	public String[] getPredecessors() {
		return predecessors;
	}
}
