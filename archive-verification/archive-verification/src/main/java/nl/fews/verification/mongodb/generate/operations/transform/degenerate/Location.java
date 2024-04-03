package nl.fews.verification.mongodb.generate.operations.transform.degenerate;

import nl.fews.verification.mongodb.generate.interfaces.IExecute;
import nl.fews.verification.mongodb.generate.interfaces.IPredecessor;
import nl.fews.verification.mongodb.generate.shared.conversion.Conversion;
import nl.fews.verification.mongodb.shared.database.Mongo;
import nl.fews.verification.mongodb.shared.io.IO;
import nl.fews.verification.mongodb.shared.settings.Settings;
import org.bson.Document;

import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.YearMonth;
import java.time.format.DateTimeFormatter;

public final class Location implements IExecute, IPredecessor {

	private final String[] predecessors = new String[]{};
	private final String study;

	public Location(String study){
		this.study = study;
	}

	/**
	 * Executes the necessary operations to generate and write template files based on retrieved documents.
	 * This method retrieves a study document from the "Verification" collection in the MongoDB database by name.
	 * It then retrieves a list of forecast names from the "Forecasts" field in the study document.
	 * For each forecast name, it retrieves a forecast document from the "Verification" collection by name.
	 * From the forecast document, it retrieves the collection, forecast name, and a list of filters.
	 * For each filter in the list, it retrieves the filter and filter name, as well as other necessary information such as forecast time, forecast start and end months, and location
	 * map.
	 * It then constructs a template string using the retrieved information and writes it to a file.
	 * The file name is generated based on the study, forecast, and filter name.
	 * The template file is written to the specified directory path in the settings.
	 */
	@Override
	public void execute(){
		Document studyDocument = Mongo.findOne("Study", new Document("Name", study));
		studyDocument.getList("Forecasts", String.class).parallelStream().forEach(s -> {
			Document forecastDocument = Mongo.findOne("Forecast", new Document("Name", s));
			String collection = forecastDocument.getString("Collection");
			DateTimeFormatter format = Conversion.getMonthDateTimeFormatter();
			String forecast = forecastDocument.getString("ForecastName");
			forecastDocument.getList("Filters", Document.class).forEach(f -> {
				String filter = f.get("Filter", Document.class).toJson();
				String filterName = f.getString("FilterName");
				String forecastTime = Conversion.getForecastTime(studyDocument.getString("Time"));
				String forecastStartMonth = YearMonth.parse(studyDocument.getString("ForecastEndMonth").isEmpty() ? LocalDateTime.now().format(format) : studyDocument.getString("ForecastEndMonth"), format).plusMonths(-1).format(format);
				String forecastEndMonth = YearMonth.parse(studyDocument.getString("ForecastEndMonth").isEmpty() ? LocalDateTime.now().format(format) : studyDocument.getString("ForecastEndMonth"), format).plusMonths(1).format(format);
				String locationMap = Conversion.getLocationMap(f.get("LocationMap", Document.class));

				String template = String.join("\n", Mongo.findOne("template.DrdlYaml", new Document("Database", Settings.get("archiveDb")).append("Type", "Degenerate").append("Name", "Location")).getList("Template", String.class));
				template = template.replace("{database}", Settings.get("archiveDb"));
				template = template.replace("{study}", study);
				template = template.replace("{forecast}", forecast);
				template = template.replace("{filterName}", filterName);
				template = template.replace("{collection}", collection);
				template = template.replace("{filter}", filter);
				template = template.replace("{forecastTime}", forecastTime);
				template = template.replace("{forecastStartMonth}", forecastStartMonth);
				template = template.replace("{forecastEndMonth}", forecastEndMonth);
				template = template.replace("{locationMap}", locationMap);

				IO.writeString(Path.of(Settings.get("drdlYamlPath"), String.format("%s_%s_Location_%s.drdl.yml", study, forecast, filterName)), template);
			});
		});
	}

	/**
	 * Retrieves an array of predecessors for the Location object.
	 *
	 * @return An array of strings representing the predecessors.
	 */
	@Override
	public String[] getPredecessors() {
		return predecessors;
	}
}
