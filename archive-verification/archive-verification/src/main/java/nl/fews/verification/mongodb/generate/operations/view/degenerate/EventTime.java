package nl.fews.verification.mongodb.generate.operations.view.degenerate;

import nl.fews.verification.mongodb.generate.interfaces.IExecute;
import nl.fews.verification.mongodb.generate.interfaces.IPredecessor;
import nl.fews.verification.mongodb.shared.database.Mongo;
import nl.fews.verification.mongodb.shared.settings.Settings;
import org.bson.Document;

import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public final class EventTime implements IExecute, IPredecessor {

	private final String[] predecessors = new String[]{};
	private final String study;

	public EventTime(String study){
		this.study = study;
	}

	@Override
	public void execute(){
		var studyDocument = Mongo.findOne("Study", new Document("Name", study));
		var environment = Settings.get("environment", String.class);
		var database = Settings.get("archiveDb", String.class);
		var name = this.getClass().getSimpleName();
		var existingCurrent = StreamSupport.stream(Mongo.find("output.View", new Document("State", "current").append("Name", name).append("Environment", environment).append("Study", study)).spliterator(), false).collect(Collectors.toMap(f -> f.getString("View"), f -> f));
		var existing = StreamSupport.stream(Mongo.listCollections(database).filter(new Document("type", "view").append("name", new Document("$regex", String.format("^view\\.verification\\.%s\\.%s\\|.+\\|%s\\|.+$", environment, study, name)))).spliterator(), false).collect(Collectors.toMap(f -> f.getString("name"), f -> f));
		var template = String.join("\n", Mongo.findOne("template.View", new Document("Type", "Degenerate").append("Name", name)).getList("Template", String.class));

		studyDocument.getList("Forecasts", String.class).parallelStream().forEach(s -> {
			var forecastDocument = Mongo.findOne("Forecast", new Document("Name", s));
			var collection = forecastDocument.getString("Collection");
			var forecast = forecastDocument.getString("ForecastName");

			forecastDocument.getList("Filters", Document.class).forEach(f -> {
				var filter = f.get("Filter", Document.class).toJson();
				var filterName = f.getString("FilterName");

				var view = String.format("view.verification.%s.%s|%s|%s|%s", environment, study, forecast, name, filterName);
				var t = template.replace("{filter}", filter);
				var document = Document.parse(String.format("{\"document\":[%s]}", t));
				if(!existing.containsKey(view)) {
					Mongo.createView(database, view, collection, document.getList("document", Document.class));
				}
				else if(existingCurrent.containsKey(view) && (!existingCurrent.get(view).get("Value").equals(document) || !existingCurrent.get(view).get("Collection").equals(collection))) {
					Mongo.dropCollection(database, view);
					Mongo.createView(database, view, collection, document.getList("document", Document.class));
				}
				Mongo.insertOne("output.View", new Document("Database", database).append("State", "new").append("View", view).append("Collection", collection).append("Name", name).append("Environment", environment).append("Study", study).append("Value", document));
			});
		});
	}

	@Override
	public String[] getPredecessors() {
		return predecessors;
	}
}