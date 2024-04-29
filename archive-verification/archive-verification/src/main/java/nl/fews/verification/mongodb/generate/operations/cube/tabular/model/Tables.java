package nl.fews.verification.mongodb.generate.operations.cube.tabular.model;

import nl.fews.verification.mongodb.generate.interfaces.IModel;
import nl.fews.verification.mongodb.generate.shared.conversion.Conversion;
import nl.fews.verification.mongodb.shared.database.Mongo;
import org.bson.Document;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
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
		var structured = String.format("%s", template.get("model", Document.class).getList("dataSources", Document.class).get(0).getString("type")).equals("structured");
		var cube = study.getString("Cube");

		if(cube.endsWith("_PowerQuery") && structured) {
			template.get("model", Document.class).getList("tables", Document.class).forEach(t ->
				t.append("partitions", StreamSupport.stream(Mongo.find("output.PowerQuery", new Document("Study", study.getString("Name")).append("Name", t.getString("name"))).spliterator(), false).map(e ->
					new Document("name", e.getString("Month").isEmpty() ? t.getString("name") : e.getString("Month")).append("source", new Document("type", "m").append("expression", e.getList("Expression", String.class)))).collect(Collectors.toList())));
		}
		else if(cube.endsWith("_PowerQuerySql") && structured) {
			template.get("model", Document.class).getList("tables", Document.class).forEach(t ->
				t.append("partitions", StreamSupport.stream(Mongo.find("output.PowerQuerySql", new Document("Study", study.getString("Name")).append("Name", t.getString("name"))).spliterator(), false).map(e ->
					new Document("name", e.getString("Month").isEmpty() ? t.getString("name") : e.getString("Month")).append("source", new Document("type", "m").append("expression", e.getList("Expression", String.class)))).collect(Collectors.toList())));
		}
		else if(cube.endsWith("_Sql") && !structured) {
			template.get("model", Document.class).getList("tables", Document.class).forEach(t ->
				t.append("partitions", StreamSupport.stream(Mongo.find("output.Sql", new Document("Study", study.getString("Name")).append("Name", t.getString("name"))).spliterator(), false).map(e ->
					new Document("name", e.getString("Month").isEmpty() ? t.getString("name") : e.getString("Month")).append("source", new Document("type", "query").append("query", Arrays.stream(e.getString("Query").replace("\r", "").split("\n")).collect(Collectors.toList())).append("dataSource", "dataSource"))).collect(Collectors.toList())));
		}
		else{
			throw new RuntimeException(String.format("%s not specified as a valid type: [ *_PowerQuery(structured) | *_PowerQuerySql(structured) | *_Sql(legacy) ]", study.getString("Cube")));
		}
		generateSeasonalityColumns();
		generateLocationColumns();
	}

	private void generateSeasonalityColumns(){
		template.get("model", Document.class).getList("tables", Document.class).stream().filter(t -> List.of("EventDate", "ForecastDate").contains(t.getString("name"))).forEach(t ->
			t.getList("columns", Document.class).addAll(study.getList("Seasonalities", String.class).stream().map(s ->
				new Document("name", String.format("%sSeason", s)).append("dataType", "string").append("sourceColumn", String.format("%sSeason", s)).append("sortByColumn", "monthOfYear")).collect(Collectors.toList())));
	}

	private void generateLocationColumns(){
		var locationAttributes = Mongo.findOne("LocationAttributes", new Document("Name", study.getString("LocationAttributes")));
		var locations = Mongo.findOne("fews.Locations", new Document());
		var locationAttributeTypes = Conversion.getLocationAttributeTypes(locations, locationAttributes);

		template.get("model", Document.class).getList("tables", Document.class).stream().filter(t -> t.getString("name").equals("Location")).forEach(t ->
			t.getList("columns", Document.class).addAll(locationAttributes.getList("Attributes", String.class).stream().map(s ->
				new Document("name", s).append("dataType", Conversion.getCubeType(locationAttributeTypes.get(s, "String"))).append("sourceColumn", s).append("displayFolder", "Attributes")).collect(Collectors.toList())));
	}
}
