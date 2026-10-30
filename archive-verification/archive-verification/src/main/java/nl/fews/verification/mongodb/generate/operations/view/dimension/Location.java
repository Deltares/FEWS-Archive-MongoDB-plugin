package nl.fews.verification.mongodb.generate.operations.view.dimension;

import nl.fews.verification.mongodb.generate.interfaces.IExecute;
import nl.fews.verification.mongodb.generate.interfaces.IPredecessor;
import nl.fews.verification.mongodb.generate.operations.view.Database;
import nl.fews.verification.mongodb.generate.shared.conversion.Conversion;
import nl.fews.verification.mongodb.shared.database.Mongo;
import nl.fews.verification.mongodb.shared.settings.Settings;
import org.bson.Document;

public final class Location implements IExecute, IPredecessor {

	private final String[] predecessors = new String[]{};
	private final String study;

	public Location(String study){
		this.study = study;
	}

	@Override
	public void execute(){
		var name = this.getClass().getSimpleName();
		var database = Settings.get("verificationDb", String.class);
		var template = String.join("\n", Mongo.findOne("template.DrdlYaml", new Document("Type", "Dimension").append("Name", name)).getList("Template", String.class));

		var studyDocument = Mongo.findOne("Study", new Document("Name", study));
		var locationAttributes = Mongo.findOne("LocationAttributes", new Document("Name", studyDocument.getString("LocationAttributes")));
		var attributes = locationAttributes.get("Attributes", Document.class);
		attributes.put("locationId", "locationId");
		attributes.put("shortName", "shortName");
		attributes.put("group", "group");

		var locations = Mongo.findOne("fews.Locations", new Document());
		var locationAttributeTypes = Conversion.getLocationAttributeTypes(locations, attributes);

		var columns = attributes.entrySet().stream().sorted((a, b) -> (a.getKey().equals("locationId") ? " " : a.getKey()).compareTo(b.getKey())).map(s -> String.format("    - Name: %s\n      MongoType: %s\n      SqlName: %s\n      SqlType: %s\n", s.getKey(), Conversion.getBsonType(locationAttributeTypes.get(s.getKey(), "String")), s.getValue(), Conversion.getSqlType(Conversion.getBsonType(locationAttributeTypes.get(s.getKey(), "String"))))).toList();

		template = template.replace("{database}", database);
		template = template.replace("{study}", study);
		template = template.replace("{columns}", String.join("", columns));
		
		Database.insertDrdlYamlToSqlView(template);
	}

	@Override
	public String[] getPredecessors() {
		return predecessors;
	}
}
