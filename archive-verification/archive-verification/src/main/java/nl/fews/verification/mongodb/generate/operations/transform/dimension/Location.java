package nl.fews.verification.mongodb.generate.operations.transform.dimension;

import nl.fews.verification.mongodb.generate.interfaces.IExecute;
import nl.fews.verification.mongodb.generate.interfaces.IPredecessor;
import nl.fews.verification.mongodb.generate.shared.conversion.Conversion;
import nl.fews.verification.mongodb.shared.database.Mongo;
import nl.fews.verification.mongodb.shared.io.IO;
import nl.fews.verification.mongodb.shared.settings.Settings;
import org.bson.Document;

import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

public final class Location implements IExecute, IPredecessor {

	private final String[] predecessors = new String[]{};
	private final String study;

	public Location(String study){
		this.study = study;
	}

	/**
	 * Executes the method.
	 *
	 * This method generates a DRDL YAML template for a specific location in a study.
	 * It retrieves a template from the "template.DrdlYaml" collection in the "Verification" database,
	 * replaces the placeholders with the actual study name and column definitions,
	 * and saves the template to a file.
	 *
	 * Pre-conditions:
	 * - The "Verification" database and its collections exist.
	 * - The "Study" collection in the "Verification" database contains a document with the specified study name.
	 * - The "LocationAttributes" collection in the "Verification" database contains a document with a name matching the location attributes specified in the study document.
	 * - The "fews.Locations" collection in the "Verification" database contains the necessary location data.
	 *
	 * Post-conditions:
	 * - The DRDL YAML template for the specified location in the study is saved to a file.
	 */
	@Override
	public void execute(){
		String template = String.join("\n", Mongo.findOne("template.DrdlYaml", new Document("Database", "Verification").append("Type", "Dimension").append("Name", "Location")).getList("Template", String.class));

		Document studyDocument = Mongo.findOne("Study", new Document("Name", study));
		Document locationAttributes = Mongo.findOne("LocationAttributes", new Document("Name", studyDocument.getString("LocationAttributes")));
		Document locations = Mongo.findOne("fews.Locations", new Document());
		Document locationAttributeTypes = Conversion.getLocationAttributeTypes(locations, locationAttributes);

		List<String> attributes = locationAttributes.getList("Attributes", String.class);
		if(!attributes.contains("locationId"))
			attributes.add(0, "locationId");
		List<String> columns = attributes.stream().map(s -> String.format("      - Name: %s\n        MongoType: %s\n        SqlName: %s\n        SqlType: %s\n", s, Conversion.getBsonType(locationAttributeTypes.getString(s)), s, Conversion.getSqlType(Conversion.getBsonType(locationAttributeTypes.getString(s))))).collect(Collectors.toList());

		template = template.replace("{study}", study);
		template = template.replace("{columns}", String.join("", columns));

		IO.writeString(Path.of(Settings.get("drdlYamlPath"), String.format("%s_Location.drdl.yml", study)).toString(), template);
	}

	/**
	 * Returns the predecessors of the Location.
	 *
	 * @return An array of strings representing the predecessors of the Location.
	 */
	@Override
	public String[] getPredecessors() {
		return predecessors;
	}
}
