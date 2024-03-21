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
import java.util.stream.StreamSupport;

public final class ForecastDate implements IExecute, IPredecessor {

	private final String[] predecessors = new String[]{};
	private final String study;

	public ForecastDate(String study){
		this.study = study;
	}

	/**
	 * Executes the method to generate and write DRDL YAML files for forecast dates.
	 * The method retrieves study and forecast information from the database,
	 * iterates through the list of forecasts, filters, and seasonalities,
	 * replaces template variables with actual values,
	 * and writes the resulting DRDL YAML template to a file.
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
				String seasonalities = Conversion.getSeasonalities(StreamSupport.stream(Mongo.find("Seasonality", new Document("Name", new Document("$in", studyDocument.getList("Seasonalities", String.class)))).spliterator(), false).toList());
				String seasonalityColumns = Conversion.getSeasonalityColumns(studyDocument.getList("Seasonalities", String.class));

				String template = String.join("\n", Mongo.findOne("template.DrdlYaml", new Document("Database", Settings.get("archiveDb")).append("Type", "Degenerate").append("Name", "ForecastDate")).getList("Template", String.class));
				template = template.replace("{database}", Settings.get("archiveDb"));
				template = template.replace("{study}", study);
				template = template.replace("{forecast}", forecast);
				template = template.replace("{filterName}", filterName);
				template = template.replace("{collection}", collection);
				template = template.replace("{filter}", filter);
				template = template.replace("{forecastTime}", forecastTime);
				template = template.replace("{forecastStartMonth}", forecastStartMonth);
				template = template.replace("{forecastEndMonth}", forecastEndMonth);
				template = template.replace("{seasonalities}", seasonalities);
				template = template.replace("{seasonalityColumns}", seasonalityColumns);

				IO.writeString(Path.of(Settings.get("drdlYamlPath"), String.format("%s_%s_ForecastDate_%s.drdl.yml", study, forecast, filterName)), template);
			});
		});
	}

	/**
	 * Retrieves the array of predecessors associated with the current object.
	 *
	 * @return an array of Strings representing the predecessors.
	 */
	@Override
	public String[] getPredecessors() {
		return predecessors;
	}
}
