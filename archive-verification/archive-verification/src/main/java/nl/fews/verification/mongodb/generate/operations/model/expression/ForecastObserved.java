package nl.fews.verification.mongodb.generate.operations.model.expression;

import nl.fews.verification.mongodb.generate.interfaces.IExecute;
import nl.fews.verification.mongodb.generate.interfaces.IPredecessor;
import nl.fews.verification.mongodb.generate.shared.conversion.Conversion;
import nl.fews.verification.mongodb.shared.database.Mongo;
import nl.fews.verification.mongodb.shared.settings.Settings;
import org.bson.Document;

import java.time.LocalDateTime;
import java.time.YearMonth;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.stream.Collectors;

public final class ForecastObserved implements IExecute, IPredecessor {

	private final String[] predecessors = new String[]{};
	private final String study;

	public ForecastObserved(String study){
		this.study = study;
	}

	/**
	 * This method is responsible for generating output power query expressions based on
	 * specific templates retrieved from a MongoDB. Its main purpose is to create dynamic
	 * power queries replacing placeholders with real values.
	 *
	 * The method works as follows:
	 *
	 * 1. Fetches a document containing Study metadata from the 'Verification' database.
	 * 2. Defines a date-time formatter and the last valid Month for which an expression needs to be generated.
	 * 3. Runs a loop for each month from the start month to the end month.
	 *    1. In each iteration retrieves a power query template from the database and gets the string for the current month in the loop.
	 *    2. Retrieves data from the 'Observed' collection to build an 'Observed' SQL query.
	 *    3. Retrieves data from the 'Forecast' collection to build a 'Forecasts' SQL query for each Forecast.
	 *    4. Retrieves data from the 'Normal' collection to build a 'Normal' SQL query.
	 *    5. The power query template is updated with real values replacing placeholders.
	 *    6. The updated template is then inserted into the 'Verification' database in the 'output.PowerQuery' collection.
	 *
	 * The power queries are created as a combination of SQL and the ODBC QUERY function provided by power query. The queries fetch
	 * data from the 'FEWS_ARCHIVE' schema in the SQL Server. A specific power query is created for every month in the given range.
	 *
	 * This method is designed to run in the context of a specific 'Study', and as such it will use details from a 'Study' document
	 * retrieved from MongoDB. Without this context, this function will fail to run successfully.
	 */
	@Override
	public void execute(){
		Document studyDocument = Mongo.findOne("Study", new Document("Name", study));
		String database = Settings.get("databaseConnectionString");
		DateTimeFormatter format = Conversion.getMonthDateTimeFormatter();
		YearMonth endMonth = YearMonth.parse(studyDocument.getString("ForecastEndMonth").isEmpty() ? LocalDateTime.now().format(format) : studyDocument.getString("ForecastEndMonth"), format);
		for(YearMonth m = YearMonth.parse(studyDocument.getString("ForecastStartMonth")); m.compareTo(endMonth) <= 0; m = m.plusMonths(1)){
			String template = String.join("\n", Mongo.findOne("template.PowerQuery", new Document("Name", "ForecastObserved")).getList("Template", String.class));
			String month = m.format(format);

			String observed = Mongo.findOne("Observed", new Document("Name", studyDocument.getString("Observed"))).getList("Filters", Document.class).stream().map(
				f -> String.format("        Odbc.Query(Source, \"SELECT * FROM %s.%s WHERE location IN (\" & Locations & \") AND endTime >= '\" & EventTimeStart & \"' AND startTime < '\" & EventTimeEnd & \"' AND eventTime >= '\" & EventTimeStart & \"' AND eventTime < '\" & EventTimeEnd & \"';\")", Settings.get("archiveDb"), String.format("%s_Observed_%s", study, f.getString("FilterName")))).collect(Collectors.joining(",\n"));

			String forecasts = studyDocument.getList("Forecasts", String.class).stream().map(s -> {
				Document forecastDocument = Mongo.findOne("Forecast", new Document("Name", s));
				return forecastDocument.getList("Filters", Document.class).stream().map(
					f -> String.format("        Odbc.Query(Source, \"SELECT * FROM %s.%s WHERE filterForecastTime >= '\" & StartDate & \"' AND filterForecastTime < '\" & EndDate & \"';\")", Settings.get("archiveDb"), String.format("%s_%s_%s", study, forecastDocument.getString("ForecastName"), f.getString("FilterName")))).collect(Collectors.joining(",\n"));
			}).collect(Collectors.joining(",\n"));

			String normal = Mongo.findOne("Normal", new Document("Name", studyDocument.getString("Normal"))).getList("Filters", Document.class).stream().map(
				f -> String.format("        Odbc.Query(Source, \"SELECT * FROM %s.%s WHERE location IN (\" & Locations & \") AND HOUR(forecastTime) IN (\" & ForecastTimeHours & \") AND endTime >= '\" & ForecastTimeStart & \"' AND startTime < '\" & ForecastTimeEnd & \"' AND forecastTime >= '\" & ForecastTimeStart & \"' AND forecastTime < '\" & ForecastTimeEnd & \"';\")", Settings.get("archiveDb"), String.format("%s_Normal_%s", study, f.getString("FilterName")))).collect(Collectors.joining(",\n"));

			template = template.replace("{database}", database);
			template = template.replace("{month}", month);
			template = template.replace("{observed}", observed);
			template = template.replace("{forecasts}", forecasts);
			template = template.replace("{normal}", normal);

			Mongo.insertOne("output.PowerQuery", new Document("Study", study).append("Name", "ForecastObserved").append("Month", month).append("Expression", Arrays.stream(template.replace("\r", "").split("\n")).toList()));
		}
	}

	/**
	 * This method returns an array of predecessors for the current object.
	 * Predecessors are determined based on the implementation provided by the concrete class.
	 *
	 * @return an array of String representing the predecessors of the object
	 */
	@Override
	public String[] getPredecessors() {
		return predecessors;
	}
}
