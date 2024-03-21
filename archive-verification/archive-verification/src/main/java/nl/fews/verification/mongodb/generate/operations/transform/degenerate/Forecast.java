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

public final class Forecast implements IExecute, IPredecessor {

	private final String[] predecessors = new String[]{};
	private final String study;

	public Forecast(String study){
		this.study = study;
	}

	/**
	 * Executes the forecast. Retrieves the study document from the "Verification" collection in the "Study" document,
	 * and retrieves the list of forecasts from the study document. For each forecast in the list, retrieves
	 * the forecast document from the "Verification" collection in the "Forecast" document, as well as other necessary
	 * information such as collection, forecast name, filters, and time range. For each filter in the filters list,
	 * replaces the placeholders in the template DRDL YAML file with the corresponding values, and writes the updated
	 * template to a file named "{study}_{forecast}_Forecast_{filterName}.drdl.yml" in the specified DRDL YAML path.
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
				String forecastStartMonth = studyDocument.getString("ForecastStartMonth");
				String forecastEndMonth = YearMonth.parse(studyDocument.getString("ForecastEndMonth").isEmpty() ? LocalDateTime.now().format(format) : studyDocument.getString("ForecastEndMonth"), format).plusMonths(1).format(format);

				String template = String.join("\n", Mongo.findOne("template.DrdlYaml", new Document("Database", Settings.get("archiveDb")).append("Type", "Degenerate").append("Name", "Forecast")).getList("Template", String.class));
				template = template.replace("{database}", Settings.get("archiveDb"));
				template = template.replace("{study}", study);
				template = template.replace("{forecast}", forecast);
				template = template.replace("{filterName}", filterName);
				template = template.replace("{collection}", collection);
				template = template.replace("{filter}", filter);
				template = template.replace("{forecastTime}", forecastTime);
				template = template.replace("{forecastStartMonth}", forecastStartMonth);
				template = template.replace("{forecastEndMonth}", forecastEndMonth);

				IO.writeString(Path.of(Settings.get("drdlYamlPath"), String.format("%s_%s_Forecast_%s.drdl.yml", study, forecast, filterName)), template);
			});
		});
	}

	/**
	 * Retrieves the predecessors of the current object.
	 *
	 * @return an array of strings representing the predecessors of the current object
	 */
	@Override
	public String[] getPredecessors() {
		return predecessors;
	}
}
