package nl.fews.verification.mongodb.generate.operations.csv.degenerate;

import nl.fews.verification.mongodb.generate.interfaces.IExecute;
import nl.fews.verification.mongodb.generate.interfaces.IPredecessor;
import nl.fews.verification.mongodb.shared.database.Mongo;
import nl.fews.verification.mongodb.shared.io.IO;
import nl.fews.verification.mongodb.shared.settings.Settings;
import org.bson.Document;

import java.nio.file.Path;
import java.util.stream.Collectors;

public final class EventTime implements IExecute, IPredecessor {

	private final String[] predecessors = new String[]{};
	private final String study;

	public EventTime(String study){
		this.study = study;
	}

	@Override
	public void execute(){
		var studyDocument = Mongo.findOne("Study", new Document("Name", study));
		var database = Settings.get("archiveDb", String.class);
		var name = this.getClass().getSimpleName();
		var pipeline = String.join("\n", Mongo.findOne("template.DrdlYaml", new Document("Type", "Degenerate").append("Name", name)).getList("Pipeline", String.class));
		var template = String.join("\n", Mongo.findOne("template.DrdlYaml", new Document("Type", "Degenerate").append("Name", name)).getList("Template", String.class));

		Document document = studyDocument.getList("Forecasts", String.class).stream().map(s -> new Document("Name", s)).reduce(new Document(), (d, s) -> {
			var forecastDocument = Mongo.findOne("Forecast", new Document("Name", s.getString("Name")));
			var collection = forecastDocument.getString("Collection");

			forecastDocument.getList("Filters", Document.class).forEach(f -> {
				var filter = f.get("Filter", Document.class).toJson();
				var t = pipeline.replace("{filter}", filter);
				var p = Document.parse(String.format("{\"document\":[%s]}", t)).getList("document", Document.class);
				if(d.isEmpty())
					d.append("pipeline", p).append("collection", collection);
				else
					d.getList("pipeline", Document.class).add(new Document("$unionWith", new Document("coll", collection).append("pipeline", p)));
			});
			return d;
		});
		var collection = document.getString("collection");
		var t = template.replace("{database}", database);
		t = t.replace("{study}", study);
		t = t.replace("{collection}", collection);
		t = t.replace("{pipeline}",  document.getList("pipeline", Document.class).stream().map(Document::toJson).collect(Collectors.joining(",\n        ")));
		IO.writeString(Path.of(Settings.get("drdlYamlPath"), String.format("%s_%s.drdl.yml", study, name)), t);
	}

	@Override
	public String[] getPredecessors() {
		return predecessors;
	}
}
