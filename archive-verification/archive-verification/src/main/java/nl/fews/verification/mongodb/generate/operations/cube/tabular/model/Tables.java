package nl.fews.verification.mongodb.generate.operations.cube.tabular.model;

import nl.fews.verification.mongodb.generate.interfaces.IModel;
import nl.fews.verification.mongodb.generate.shared.conversion.Conversion;
import nl.fews.verification.mongodb.shared.database.Mongo;
import org.bson.Document;

import java.util.Comparator;
import java.util.List;
import java.util.stream.StreamSupport;

public class Tables implements IModel {
	private final Document study;
	private final Document template;

	public Tables(Document study, Document template){
		this.study = study;
		this.template = template;
	}

	@Override
	public void generate() {
		template.get("model", Document.class).getList("tables", Document.class).forEach(t ->
			t.append("partitions", StreamSupport.stream(Mongo.find("output.PowerQuery", new Document("Study", study.getString("Name")).append("Name", t.getString("name"))).spliterator(), false).sorted(Comparator.comparing(e -> e.getString("Month"))).map(e ->
				new Document("name", e.getString("Month").isEmpty() ? t.getString("name") : e.getString("Month")).append("source", new Document("type", "m").append("expression", e.getList("Expression", String.class)))).toList()));

		generateSeasonalityColumns();
		generateLocationColumns();
	}

	private void generateSeasonalityColumns(){
		template.get("model", Document.class).getList("tables", Document.class).stream().filter(t -> List.of("EventDate", "ForecastDate").contains(t.getString("name"))).forEach(t ->
			t.getList("columns", Document.class).addAll(study.getList("Seasonalities", String.class).stream().map(s ->
				new Document("name", String.format("%sSeason", s)).append("dataType", "string").append("sourceColumn", String.format("%sSeason", s)).append("sortByColumn", "monthOfYear")).toList()));
	}

	private void generateLocationColumns(){
		var locationAttributes = Mongo.findOne("LocationAttributes", new Document("Name", study.getString("LocationAttributes")));
		var attributes = locationAttributes.get("Attributes", Document.class);
		attributes.put("locationId", "locationId");
		attributes.put("shortName", "shortName");
		attributes.put("group", "group");

		var locations = Mongo.findOne("fews.Locations", new Document());
		var locationAttributeTypes = Conversion.getLocationAttributeTypes(locations, attributes);

		template.get("model", Document.class).getList("tables", Document.class).stream().filter(t -> t.getString("name").equals("Location")).forEach(t ->
			t.getList("columns", Document.class).addAll(attributes.entrySet().stream().map(s ->
				new Document("name", s.getValue()).append("dataType", Conversion.getCubeType(locationAttributeTypes.get(s.getKey(), "String"))).append("sourceColumn", s.getValue()).append("displayFolder", "Attributes")).toList()));
	}
}
