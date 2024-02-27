package nl.fews.verification.mongodb.generate.operations.transform.fact;

import nl.fews.verification.mongodb.generate.interfaces.IExecute;
import nl.fews.verification.mongodb.generate.interfaces.IPredecessor;
import nl.fews.verification.mongodb.generate.shared.conversion.Conversion;
import nl.fews.verification.mongodb.shared.database.Mongo;
import nl.fews.verification.mongodb.shared.io.IO;
import nl.fews.verification.mongodb.shared.settings.Settings;
import org.bson.Document;

import java.nio.file.Path;

public final class Observed implements IExecute, IPredecessor {

	private final String[] predecessors = new String[]{};
	private final String study;

	public Observed(String study){
		this.study = study;
	}

	/**
	 * Executes the method by retrieving study and observed information from the "Verification" collection in the database.
	 * It then iterates over the list of filters for the observed document and performs various string replacements in a template string.
	 * Finally, the resulting template is written to a file in the specified drdlYamlPath.
	 */
	@Override
	public void execute(){
		Document studyDocument = Mongo.findOne("Study", new Document("Name", study));
		Document observedDocument = Mongo.findOne("Observed", new Document("Name", studyDocument.getString("Observed")));
		String collection = observedDocument.getString("Collection");
		observedDocument.getList("Filters", Document.class).forEach(f -> {
			String filter = Conversion.getFilter(f.get("Filter", Document.class));
			String filterName = f.getString("FilterName");
			String startTime = Conversion.getStartTime(studyDocument.getString("Time"));
			String endTime = Conversion.getEndTime(studyDocument.getString("Time"));
			String eventTime = Conversion.getEventTime(studyDocument.getString("Time"));
			String eventValue= Conversion.getEventValue(studyDocument.getString("Value"));
			String locationMap = Conversion.getLocationMap(f.get("LocationMap", Document.class));
			String observedClass = Conversion.getObservedClass(Mongo.findOne("Class", new Document("Name", studyDocument.getString("Class"))));

			String template = String.join("\n", Mongo.findOne("template.DrdlYaml", new Document("Database", Settings.get("archiveDB")).append("Type", "Fact").append("Name", "Observed")).getList("Template", String.class));
			template = template.replace("{study}", study);
			template = template.replace("{filterName}", filterName);
			template = template.replace("{collection}", collection);
			template = template.replace("{filter}", filter);
			template = template.replace("{startTime}", startTime);
			template = template.replace("{endTime}", endTime);
			template = template.replace("{eventTime}", eventTime);
			template = template.replace("{eventValue}", eventValue);
			template = template.replace("{locationMap}", locationMap);
			template = template.replace("{observedClass}", observedClass);

			IO.writeString(Path.of(Settings.get("drdlYamlPath"), String.format("%s_Observed_%s.drdl.yml", study, filterName)).toString(), template);
		});
	}

	/**
	 * Returns an array of strings representing the predecessors of the object.
	 *
	 * @return An array of strings representing the predecessors.
	 */
	@Override
	public String[] getPredecessors() {
		return predecessors;
	}
}
