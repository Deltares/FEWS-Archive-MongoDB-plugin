package nl.fews.verification.mongodb.generate.operations.drdlyaml.dimension;

import nl.fews.verification.mongodb.generate.interfaces.IExecute;
import nl.fews.verification.mongodb.generate.interfaces.IPredecessor;
import nl.fews.verification.mongodb.generate.shared.conversion.Conversion;
import nl.fews.verification.mongodb.shared.database.Mongo;
import nl.fews.verification.mongodb.shared.io.IO;
import nl.fews.verification.mongodb.shared.settings.Settings;
import org.bson.Document;

import java.nio.file.Path;
import java.util.List;

public final class Location implements IExecute, IPredecessor {

	private final String[] predecessors = new String[]{};
	private final String study;

	public Location(String study){
		this.study = study;
	}

	@Override
	public void execute(){
		var database = Settings.get("verificationDb", String.class);
		var name = this.getClass().getSimpleName();
		var template = String.join("\n", Mongo.findOne("template.DrdlYaml", new Document("Type", "Dimension").append("Name", name)).getList("Template", String.class));

		var studyDocument = Mongo.findOne("Study", new Document("Name", study));
		var locationAttributes = Mongo.findOne("LocationAttributes", new Document("Name", studyDocument.getString("LocationAttributes")));
		var locations = Mongo.findOne("fews.Locations", new Document());
		var locationAttributeTypes = Conversion.getLocationAttributeTypes(locations, locationAttributes);

		var attributes = locationAttributes.getList("Attributes", String.class);
		if(!attributes.contains("locationId"))
			attributes.add(0, "locationId");
		var columns = attributes.stream().map(s -> String.format("    - Name: %s\n      MongoType: %s\n      SqlName: %s\n      SqlType: %s\n", s, Conversion.getBsonType(locationAttributeTypes.get(s, "String")), s, Conversion.getSqlType(Conversion.getBsonType(locationAttributeTypes.get(s, "String"))))).toList();

		template = template.replace("{database}", database);
		template = template.replace("{study}", study);
		template = template.replace("{columns}", String.join("", columns));

		IO.writeString(Path.of(Settings.get("drdlYamlPath"), String.format("%s_%s_%s.drdl.yml", study, name, database)), template);
	}

	@Override
	public String[] getPredecessors() {
		return predecessors;
	}
}
