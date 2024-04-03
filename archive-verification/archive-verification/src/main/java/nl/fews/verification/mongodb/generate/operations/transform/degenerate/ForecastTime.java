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

public final class ForecastTime implements IExecute, IPredecessor {

	private final String[] predecessors = new String[]{};
	private final String study;

	public ForecastTime(String study){
		this.study = study;
	}

	/**
	 * Executes the method.
	 *
	 * This method retrieves study information from the "Verification" collection in the Mongo database.
	 * It then retrieves the list of forecasts associated with the study and processes each forecast in parallel.
	 * For each forecast, it retrieves the forecast document from the "Verification" collection and extracts the necessary information.
	 * It then iterates over the list of filters for the forecast and performs some operations on each filter.
	 *
	 * The method replaces placeholders in a template using the extracted information and writes the resulting template to a file.
	 * The placeholders that are replaced include "{study}", "{forecast}", "{filterName}", "{collection}", "{filter}",
	 * "{forecastTime}", "{forecastStartMonth}", and "{forecastEndMonth}".
	 * The template is retrieved from the "Verification" collection in the Mongo database.
	 * The generated file is saved to a path specified in the "drdlYamlPath" setting.
	 * The file name is constructed using the study name, forecast name, filter name, and a fixed suffix.
	 *
	 * @see ForecastTime
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

				String template = String.join("\n", Mongo.findOne("template.DrdlYaml", new Document("Database", Settings.get("archiveDb")).append("Type", "Degenerate").append("Name", "ForecastTime")).getList("Template", String.class));
				template = template.replace("{database}", Settings.get("archiveDb"));
				template = template.replace("{study}", study);
				template = template.replace("{forecast}", forecast);
				template = template.replace("{filterName}", filterName);
				template = template.replace("{collection}", collection);
				template = template.replace("{filter}", filter);
				template = template.replace("{forecastTime}", forecastTime);
				template = template.replace("{forecastStartMonth}", forecastStartMonth);
				template = template.replace("{forecastEndMonth}", forecastEndMonth);

				IO.writeString(Path.of(Settings.get("drdlYamlPath"), String.format("%s_%s_ForecastTime_%s.drdl.yml", study, forecast, filterName)), template);
			});
		});
	}

	/**
	 * Retrieves the array of predecessors associated with this object.
	 *
	 * @return The array of predecessors.
	 */
	@Override
	public String[] getPredecessors() {
		return predecessors;
	}
}
