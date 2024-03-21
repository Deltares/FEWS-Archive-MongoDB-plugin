package nl.fews.verification.mongodb.generate.operations.transform.fact;

import nl.fews.verification.mongodb.generate.interfaces.IExecute;
import nl.fews.verification.mongodb.generate.interfaces.IPredecessor;
import nl.fews.verification.mongodb.generate.shared.conversion.Conversion;
import nl.fews.verification.mongodb.shared.database.Mongo;
import nl.fews.verification.mongodb.shared.io.IO;
import nl.fews.verification.mongodb.shared.settings.Settings;
import org.bson.Document;

import java.nio.file.Path;

public final class Normal implements IExecute, IPredecessor {

	private final String[] predecessors = new String[]{};
	private final String study;

	public Normal(String study){
		this.study = study;
	}

	/**
	 * Executes the method to generate and write drdl yaml files based on the study, filters, and other parameters.
	 * It retrieves the study document and normal document from the "Verification" MongoDB collection.
	 * Then, it iterates over the list of filters in the normal document and generates a drdl yaml file for each filter.
	 * The generated yaml file is based on the template retrieved from the "Verification" collection, with placeholders replaced by the actual values.
	 * The generated yaml file is written to the drdlYamlPath directory with a filename based on the study and filter name.
	 * This method does not return any value.
	 */
	@Override
	public void execute(){
		Document studyDocument = Mongo.findOne("Study", new Document("Name", study));
		Document normalDocument = Mongo.findOne("Normal", new Document("Name", studyDocument.getString("Normal")));
		String collection = normalDocument.getString("Collection");
		String generateDays = normalDocument.getInteger("GenerateDays").toString();
		normalDocument.getList("Filters", Document.class).forEach(f -> {
			String filter = f.get("Filter", Document.class).toJson();
			String filterName = f.getString("FilterName");
			String startTime = Conversion.getStartTime(studyDocument.getString("Time"));
			String endTime = Conversion.getEndTime(studyDocument.getString("Time"));
			String forecastTime = Conversion.getForecastTime(studyDocument.getString("Time"));
			String eventTime = Conversion.getEventTime(studyDocument.getString("Time"));
			String eventValue = Conversion.getEventValue(studyDocument.getString("Value"));
			String locationMap = Conversion.getLocationMap(f.get("LocationMap", Document.class));
			String forecastClass = Conversion.getForecastClass(Mongo.findOne("Class", new Document("Name", studyDocument.getString("Class"))));

			String template = String.join("\n", Mongo.findOne("template.DrdlYaml", new Document("Database", Settings.get("archiveDb")).append("Type", "Fact").append("Name", "Normal")).getList("Template", String.class));
			template = template.replace("{database}", Settings.get("archiveDb"));
			template = template.replace("{study}", study);
			template = template.replace("{filterName}", filterName);
			template = template.replace("{collection}", collection);
			template = template.replace("{filter}", filter);
			template = template.replace("{startTime}", startTime);
			template = template.replace("{endTime}", endTime);
			template = template.replace("{forecastTime}", forecastTime);
			template = template.replace("{eventTime}", eventTime);
			template = template.replace("{eventValue}", eventValue);
			template = template.replace("{locationMap}", locationMap);
			template = template.replace("{forecastClass}", forecastClass);
			template = template.replace("{generateDays}", generateDays);

			IO.writeString(Path.of(Settings.get("drdlYamlPath"), String.format("%s_Normal_%s.drdl.yml", study, filterName)), template);
		});
	}

	/**
	 * Retrieves the predecessors of the current object.
	 *
	 * @return An array of strings representing the predecessors of the current object.
	 */
	@Override
	public String[] getPredecessors() {
		return predecessors;
	}
}
