package nl.fews.verification.mongodb.generate.operations.model.expression;

import nl.fews.verification.mongodb.generate.interfaces.IExecute;
import nl.fews.verification.mongodb.generate.interfaces.IPredecessor;
import nl.fews.verification.mongodb.shared.database.Mongo;
import nl.fews.verification.mongodb.shared.settings.Settings;
import org.bson.Document;

import java.util.Arrays;
import java.util.stream.Collectors;

public final class EventTime implements IExecute, IPredecessor {

	private final String[] predecessors = new String[]{};
	private final String study;

	public EventTime(String study){
		this.study = study;
	}

	/**
	 * The `execute` method generates an output PowerQuery document relating to "EventTime". It fetches
	 * a study document and a template from the 'Verification' database within MongoDB, creates a series
	 * of SQL queries based on the forecasts and filters within the study, and replaces placeholders in
	 * the template with this generated query and the database connection string from the application settings.
	 *
	 * The detailed operation of the method is as follows:
	 *
	 * 1. Gets a study document from the `Verification` database in MongoDB. The document is fetched
	 *    from the `Study` collection where the `Name` field matches the study.
	 * 2. Gets a PowerQuery template from the `Verification` database in MongoDB. The document is fetched
	 *    from the `template.PowerQuery` collection where the `Name` field matches the string "EventTime".
	 * 3. The template is processed to a string used in the final step for the `Expression` field.
	 * 4. Retrieves the database connection string from the settings.
	 * 5. The method launches a series of transformations that generate an SQL command string. This string
	 *    is tailored based on the forecast names and filter names contained within the study document.
	 * 6. The placeholders in the template string gotten earlier are replaced with the database connection
	 *    string and the generated SQL command string.
	 * 7. Finally, the populated template string is inserted back into MongoDB, in the `Verification`
	 *    database under the `output.PowerQuery` collection. The new document contains the study name,
	 *    month, and the final populated template in the `Expression` field.
	 *
	 * Important: This method heavily utilizes MongoDB querying capability and is dependent on pulling,
	 * processing, and pushing data in and out of MongoDB. Most of its workload is focused on string
	 * manipulations and working with Java Streams.
	 */
	@Override
	public void execute(){
		Document studyDocument = Mongo.findOne("Study", new Document("Name", study));
		String template = String.join("\n", Mongo.findOne("template.PowerQuery", new Document("Name", "EventTime")).getList("Template", String.class));
		String database = Settings.get("databaseConnectionString");
		String queries = studyDocument.getList("Forecasts", String.class).stream().map(s -> {
			Document forecastDocument = Mongo.findOne("Forecast", new Document("Name", s));
			return forecastDocument.getList("Filters", Document.class).stream().map(
				f -> String.format("        Odbc.Query(Source, \"SELECT * FROM %s.%s\")", Settings.get("archiveDb"), String.format("%s_%s_EventTime_%s", study, forecastDocument.getString("ForecastName"), f.getString("FilterName")))).collect(Collectors.joining(",\n"));
		}).collect(Collectors.joining(",\n"));

		template = template.replace("{database}", database);
		template = template.replace("{queries}", queries);

		Mongo.insertOne("output.PowerQuery", new Document("Study", study).append("Name", "EventTime").append("Month", "").append("Expression", Arrays.stream(template.replace("\r", "").split("\n")).toList()));
	}

	/**
	 * Retrieves an array of predecessors associated with the current object.
	 *
	 * @return An array of strings representing the predecessors of the object.
	 */
	@Override
	public String[] getPredecessors() {
		return predecessors;
	}
}
