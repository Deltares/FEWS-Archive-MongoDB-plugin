package nl.fews.verification.mongodb.generate.operations.model.expression;

import nl.fews.verification.mongodb.generate.interfaces.IExecute;
import nl.fews.verification.mongodb.generate.interfaces.IPredecessor;
import nl.fews.verification.mongodb.shared.database.Mongo;
import nl.fews.verification.mongodb.shared.settings.Settings;
import org.bson.Document;

import java.util.Arrays;
import java.util.stream.Collectors;

public final class Location implements IExecute, IPredecessor {

	private final String[] predecessors = new String[]{};
	private final String study;

	public Location(String study){
		this.study = study;
	}

	/**
	 * Executes the method to generate a PowerQuery expression for the "Location" template.
	 *
	 * This method retrieves the study information from MongoDB and generates a PowerQuery expression using a template.
	 * The generated expression is then inserted into the MongoDB "output.PowerQuery" collection.
	 *
	 * @throws NullPointerException if the input study does not exist in the "Verification.Study" collection
	 */
	@Override
	public void execute(){
		Document studyDocument = Mongo.findOne("Study", new Document("Name", study));
		String template = String.join("\n", Mongo.findOne("template.PowerQuery", new Document("Name", "Location")).getList("Template", String.class));
		String database = Settings.get("databaseConnectionString");
		String queries = studyDocument.getList("Forecasts", String.class).stream().map(s -> {
			Document forecastDocument = Mongo.findOne("Forecast", new Document("Name", s));
			return forecastDocument.getList("Filters", Document.class).stream().map(
				f -> String.format("        SELECT s.location, l.* FROM %s.`%s` s INNER JOIN %s.`%s` l ON l.locationId=s.locationId", Settings.get("archiveDb"), String.format("%s_%s_Location_%s", study, forecastDocument.getString("ForecastName"), f.getString("FilterName")), Settings.get("verificationDb"), String.format("%s_Location", study))).collect(Collectors.joining(" UNION ALL\n"));
		}).collect(Collectors.joining(" UNION ALL\n"));

		template = template.replace("{database}", database);
		template = template.replace("{queries}", queries);

		Mongo.insertOne("output.PowerQuery", new Document("Study", study).append("Name", "Location").append("Month", "").append("Expression", Arrays.stream(template.replace("\r", "").split("\n")).toList()));
	}

	/**
	 * Returns the array of predecessors for this Location object.
	 *
	 * @return the array of predecessors for this Location object
	 */
	@Override
	public String[] getPredecessors() {
		return predecessors;
	}
}
