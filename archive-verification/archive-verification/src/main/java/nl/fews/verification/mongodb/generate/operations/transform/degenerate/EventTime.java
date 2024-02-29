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

public final class EventTime implements IExecute, IPredecessor {

	private final String[] predecessors = new String[]{};
	private final String study;

	public EventTime(String study){
		this.study = study;
	}

	/**
	 * Executes the EventTime process.
	 * Retrieves study document from the "Verification.Study" collection based on the given study name.
	 * For each forecast in the "Forecasts" list of the study document, retrieves the forecast document from the "Verification.Forecast" collection.
	 * Retrieves the collection, forecast name, and filters from the forecast document.
	 * For each filter, retrieves the filter and filter name.
	 * Retrieves the forecast time, event time, forecast start month, and forecast end month from the study document.
	 * Modifies a template YAML file by replacing placeholders with the retrieved values.
	 * Writes the modified template to a new file named "{study}_{forecast}_EventTime_{filterName}.drdl.yml" in the specified directory.
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

				String template = String.join("\n", Mongo.findOne("template.DrdlYaml", new Document("Database", Settings.get("archiveDb")).append("Type", "Degenerate").append("Name", "EventTime")).getList("Template", String.class));
				template = template.replace("{study}", study);
				template = template.replace("{forecast}", forecast);
				template = template.replace("{filterName}", filterName);
				template = template.replace("{collection}", collection);
				template = template.replace("{filter}", filter);
				template = template.replace("{forecastTime}", forecastTime);
				template = template.replace("{eventTime}", eventTime);
				template = template.replace("{forecastStartMonth}", forecastStartMonth);
				template = template.replace("{forecastEndMonth}", forecastEndMonth);

				IO.writeString(Path.of(Settings.get("drdlYamlPath"), String.format("%s_%s_EventTime_%s.drdl.yml", study, forecast, filterName)).toString(), template);
			});
		});
	}

	/**
	 * Returns the predecessors of the current process.
	 *
	 * @return an array of strings representing the predecessors of the current process
	 */
	@Override
	public String[] getPredecessors() {
		return predecessors;
	}
}
