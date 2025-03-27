package nl.fews.verification.mongodb.generate.operations.drdlyaml.fact;

import nl.fews.verification.mongodb.generate.interfaces.IExecute;
import nl.fews.verification.mongodb.generate.interfaces.IPredecessor;
import nl.fews.verification.mongodb.generate.shared.conversion.Conversion;
import nl.fews.verification.mongodb.shared.database.Mongo;
import nl.fews.verification.mongodb.shared.io.IO;
import nl.fews.verification.mongodb.shared.settings.Settings;
import org.bson.Document;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.stream.Collectors;

public final class Forecasts implements IExecute, IPredecessor {

	private final String[] predecessors = new String[]{};
	private final String study;

	public Forecasts(String study){
		this.study = study;
	}

	@Override
	public void execute(){
		var studyDocument = Mongo.findOne("Study", new Document("Name", study));
		var database = Settings.get("archiveDb", String.class);
		var name = this.getClass().getSimpleName();
		var pipeline = String.join("\n", Mongo.findOne("template.DrdlYaml", new Document("Type", "Fact").append("Name", name)).getList("Pipeline", String.class));
		var template = String.join("\n", Mongo.findOne("template.DrdlYaml", new Document("Type", "Fact").append("Name", name)).getList("Template", String.class));

		var forecastTime = Conversion.getForecastTime(studyDocument.getString("Time"));
		var eventTime = Conversion.getEventTime(studyDocument.getString("Time"));
		var eventValue = Conversion.getEventValue(studyDocument.getString("Value"));

		var forecastClass = Conversion.getForecastClass(studyDocument.getString("Class").isEmpty() ? null : Mongo.findOne("Class", new Document("Name", studyDocument.getString("Class"))));

		var document = studyDocument.getList("Forecasts", String.class).stream().map(s -> new Document("Name", s)).reduce(new Document(), (d, s) -> {
			var forecastDocument = Mongo.findOne("Forecast", new Document("Name", s.getString("Name")));
			var collection = forecastDocument.getString("Collection");
			var forecast = forecastDocument.getString("ForecastName");

			forecastDocument.getList("Filters", Document.class).forEach(f -> {
				var filter = f.get("Filter", Document.class);
				var locationMap = Conversion.getLocationMap(f.get("LocationMap", Document.class));

				var t = pipeline.replace("{filter}", filter.toJson());
				t = t.replace("{forecast}", forecast);
				t = t.replace("{forecastTime}", forecastTime);
				t = t.replace("{eventTime}", eventTime);
				t = t.replace("{eventValue}", eventValue);
				t = t.replace("{locationMap}", locationMap);
				t = t.replace("{forecastClass}", forecastClass);
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
		
		if(studyDocument.getString("Cube").equals("Default"))
			IO.writeString(Path.of(Settings.get("drdlYamlPath"), String.format("%s_%s.drdl.yml", study, name)), t);
		else if (studyDocument.getString("Cube").equals("Csv"))
			Mongo.insertOne("output.DrdlYaml", new Document("Study", study).append("Name", String.format("%s_%s", study, name)).append("Expression", Arrays.stream(t.replace("\r", "").split("\n")).toList()));
	}

	@Override
	public String[] getPredecessors() {
		return predecessors;
	}
}