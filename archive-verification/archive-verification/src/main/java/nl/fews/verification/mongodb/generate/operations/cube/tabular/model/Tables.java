package nl.fews.verification.mongodb.generate.operations.cube.tabular.model;

import nl.fews.verification.mongodb.generate.interfaces.IModel;
import nl.fews.verification.mongodb.generate.shared.conversion.Conversion;
import nl.fews.verification.mongodb.shared.database.Mongo;
import org.bson.Document;

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

	/**
	 * The `generate` method is pivotal for the generation of model tables and their corresponding partitions. It operates by fetching and transforming necessary data from MongoDB.
	 * The procedure of this method is explained more thoroughly in the following steps:
	 *
	 * 1. Iterates over every table in the `model` section of the profile-defined `template`. Each table is, in turn, appended with `partitions` resulting from a sequential stream of MongoDB query results. Each partition corresponds to a unique table.
	 * 2. Each MongoDB query searches the `Verification` database, specifically the `output.PowerQuery` collection, to find documents of the same "Study" and "Name". This is done to obtain a list of documents related to a specific month in the `output.PowerQuery` collection.
	 * 3. For all retrieved MongoDB documents, it generates a new `Document` object. The `name` field of this object gets the name of the month from the retrieved MongoDB document if it exists, otherwise it nabs the name from the model table in the `template`. The `source` field of this object gets populated with a new `Document` that has the `type` set to "m" and `expression` set to the list of expressions acquired from the retrieved MongoDB document.
	 * 4. It then invokes the `generateSeasonalityColumns()` method. (Documentation for this can be provided separately)
	 * 5. It subsequently invokes the `generateLocationColumns()` method. (Documentation for this can be provided separately)
	 *
	 * Please note that this method is hinged upon MongoDB for data extraction, along with Java Streams API for data transformation.
	 */
	@Override
	public void generate() {
		template.get("model", Document.class).getList("tables", Document.class).forEach(t ->
			t.append("partitions", StreamSupport.stream(Mongo.find("output.PowerQuery", new Document("Study", study.getString("Name")).append("Name", t.getString("name"))).spliterator(), false).map(e ->
				new Document("name", e.getString("Month").isEmpty() ? t.getString("name") : e.getString("Month")).append("source", new Document("type", "m").append("expression", e.getList("Expression", String.class)))).collect(Collectors.toList())));

		generateSeasonalityColumns();
		generateLocationColumns();
	}

	/**
	 * Generates seasonality columns based on the list of seasonalities in the study document.
	 * Each seasonality will have a corresponding column added to the tables.
	 */
	private void generateSeasonalityColumns(){
		template.get("model", Document.class).getList("tables", Document.class).stream().filter(t -> List.of("EventDate", "ForecastDate").contains(t.getString("name"))).forEach(t ->
			t.getList("columns", Document.class).addAll(study.getList("Seasonalities", String.class).stream().map(s ->
				new Document("name", String.format("%sSeason", s)).append("dataType", "string").append("sourceColumn", String.format("%sSeason", s)).append("sortByColumn", "monthOfYear")).collect(Collectors.toList())));
	}

	/**
	 * Generates location columns for the "Location" table in the model.
	 * It retrieves location attributes and their types from the database,
	 * and adds columns for each attribute to the "Location" table.
	 */
	private void generateLocationColumns(){
		Document locationAttributes = Mongo.findOne("LocationAttributes", new Document("Name", study.getString("LocationAttributes")));
		Document locations = Mongo.findOne("fews.Locations", new Document());
		Document locationAttributeTypes = Conversion.getLocationAttributeTypes(locations, locationAttributes);

		template.get("model", Document.class).getList("tables", Document.class).stream().filter(t -> t.getString("name").equals("Location")).forEach(t ->
			t.getList("columns", Document.class).addAll(locationAttributes.getList("Attributes", String.class).stream().map(s ->
				new Document("name", s).append("dataType", Conversion.getCubeType(locationAttributeTypes.get(s, "String"))).append("sourceColumn", s).append("displayFolder", "Attributes")).collect(Collectors.toList())));
	}
}
