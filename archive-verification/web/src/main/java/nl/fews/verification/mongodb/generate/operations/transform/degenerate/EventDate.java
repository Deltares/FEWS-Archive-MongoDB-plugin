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
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public final class EventDate implements IExecute, IPredecessor {

	private final String[] predecessors = new String[]{};
	private final String study;

	public EventDate(String study){
		this.study = study;
	}

	/**
	 * Executes the method by performing the following steps:
	 * 1. Retrieves the study document from the "Verification" collection, "Study" document.
	 * 2. Iterates through the list of forecasts in the study document.
	 * 3. For each forecast, retrieves the forecast document from the "Verification" collection, "Forecast" document.
	 * 4. Retrieves the collection, date and filter information from the forecast document.
	 * 5. Iterates through the list of filters in the forecast document.
	 * 6. For each filter, retrieves the filter and filter name.
	 * 7. Retrieves the forecast time, event time, forecast start month, forecast end month,
	 *    seasonalities, and seasonality columns from the study document.
	 * 8. Retrieves the DRDL YAML template from the "Verification" collection, "template.DrdlYaml" document.
	 * 9. Replaces the placeholders in the template with the corresponding values.
	 * 10. Writes the modified template to a file with a specific filename based on the study, forecast, and filter name.
	 *
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
				String filter = Conversion.getFilter(f.get("Filter", Document.class));
				String filterName = f.getString("FilterName");
				String forecastTime = Conversion.getForecastTime(studyDocument.getString("Time"));
				String eventTime = Conversion.getEventTime(studyDocument.getString("Time"));
				String forecastStartMonth = studyDocument.getString("ForecastStartMonth");
				String forecastEndMonth = YearMonth.parse(studyDocument.getString("ForecastEndMonth") == null ? LocalDateTime.now().format(format) : studyDocument.getString("ForecastEndMonth"), format).plusMonths(1).format(format);
				String seasonalities = Conversion.getSeasonalities(StreamSupport.stream(Mongo.find("Seasonality", new Document("Name", new Document("$in", studyDocument.getList("Seasonalities", String.class)))).spliterator(), false).collect(Collectors.toList()));
				String seasonalityColumns = Conversion.getSeasonalityColumns(studyDocument.getList("Seasonalities", String.class));

				String template = String.join("\n", Mongo.findOne("template.DrdlYaml", new Document("Database", Settings.get("archiveDb")).append("Type", "Degenerate").append("Name", "EventDate")).getList("Template", String.class));
				template = template.replace("{study}", study);
				template = template.replace("{forecast}", forecast);
				template = template.replace("{filterName}", filterName);
				template = template.replace("{collection}", collection);
				template = template.replace("{filter}", filter);
				template = template.replace("{forecastTime}", forecastTime);
				template = template.replace("{eventTime}", eventTime);
				template = template.replace("{forecastStartMonth}", forecastStartMonth);
				template = template.replace("{forecastEndMonth}", forecastEndMonth);
				template = template.replace("{seasonalities}", seasonalities);
				template = template.replace("{seasonalityColumns}", seasonalityColumns);

				IO.writeString(Path.of(Settings.get("drdlYamlPath"), String.format("%s_%s_EventDate_%s.drdl.yml", study, forecast, filterName)).toString(), template);
			});
		});
	}

	/**
	 * Returns an array of predecessors.
	 *
	 * @return an array of String representing the predecessors
	 */
	@Override
	public String[] getPredecessors() {
		return predecessors;
	}
}
