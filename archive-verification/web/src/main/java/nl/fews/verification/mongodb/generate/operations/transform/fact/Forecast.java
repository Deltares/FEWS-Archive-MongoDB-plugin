package nl.fews.verification.mongodb.generate.operations.transform.fact;

import nl.fews.verification.mongodb.generate.interfaces.IExecute;
import nl.fews.verification.mongodb.generate.interfaces.IPredecessor;
import nl.fews.verification.mongodb.generate.shared.conversion.Conversion;
import nl.fews.verification.mongodb.shared.database.Mongo;
import nl.fews.verification.mongodb.shared.io.IO;
import nl.fews.verification.mongodb.shared.settings.Settings;
import org.bson.Document;

import java.nio.file.Path;

public final class Forecast implements IExecute, IPredecessor {

	private final String[] predecessors = new String[]{};
	private final String study;

	public Forecast(String study){
		this.study = study;
	}

	/**
	 * Executes the forecast process.
	 * This method finds the study document in the "Verification" collection based on the given study name.
	 * It then retrieves the list of forecasts associated with the study and processes each forecast in parallel.
	 * For each forecast, it retrieves the forecast document from the "Verification" collection based on the forecast name.
	 * It extracts the necessary information from the forecast document such as collection, forecast name, filters, etc.
	 * It then generates a template using the extracted information and replaces the placeholders in the template with the actual values.
	 * Finally, it writes the generated template to a file named "{study}_{forecast}_{filterName}.drdl.yml" in the specified directory.
	 */
	@Override
	public void execute(){
		Document studyDocument = Mongo.findOne("Study", new Document("Name", study));
		studyDocument.getList("Forecasts", String.class).parallelStream().forEach(s -> {
			Document forecastDocument = Mongo.findOne("Forecast", new Document("Name", s));
			String collection = forecastDocument.getString("Collection");
			String forecast = forecastDocument.getString("ForecastName");
			forecastDocument.getList("Filters", Document.class).forEach(f -> {
				String filter = Conversion.getFilter(f.get("Filter", Document.class));
				String filterName = f.getString("FilterName");
				String forecastTime = Conversion.getForecastTime(studyDocument.getString("Time"));
				String eventTime = Conversion.getEventTime(studyDocument.getString("Time"));
				String eventValue = Conversion.getEventValue(studyDocument.getString("Value"));
				String locationMap = Conversion.getLocationMap(f.get("LocationMap", Document.class));
				String forecastClass = Conversion.getForecastClass(Mongo.findOne("Class", new Document("Name", studyDocument.getString("Class"))));

				String template = String.join("\n", Mongo.findOne("template.DrdlYaml", new Document("Database", Settings.get("archiveDB")).append("Type", "Fact").append("Name", "Forecast")).getList("Template", String.class));
				template = template.replace("{study}", study);
				template = template.replace("{forecast}", forecast);
				template = template.replace("{filterName}", filterName);
				template = template.replace("{collection}", collection);
				template = template.replace("{filter}", filter);
				template = template.replace("{forecastTime}", forecastTime);
				template = template.replace("{eventTime}", eventTime);
				template = template.replace("{eventValue}", eventValue);
				template = template.replace("{locationMap}", locationMap);
				template = template.replace("{forecastClass}", forecastClass);

				IO.writeString(Path.of(Settings.get("drdlYamlPath"), String.format("%s_%s_%s.drdl.yml", study, forecast, filterName)).toString(), template);
			});
		});
	}

	/**
	 * Retrieves the predecessors of the current object.
	 *
	 * @return An array of Strings representing the predecessors of the current object.
	 */
	@Override
	public String[] getPredecessors() {
		return predecessors;
	}
}
