package nl.fews.verification.mongodb.generate.operations.csv.degenerate;

import nl.fews.verification.mongodb.generate.interfaces.IExecute;
import nl.fews.verification.mongodb.generate.interfaces.IPredecessor;
import nl.fews.verification.mongodb.generate.shared.conversion.Conversion;
import nl.fews.verification.mongodb.shared.database.Mongo;
import nl.fews.verification.mongodb.shared.io.IO;
import nl.fews.verification.mongodb.shared.settings.Settings;
import org.bson.Document;

import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.YearMonth;
import java.util.stream.Collectors;

public final class LeadTime implements IExecute, IPredecessor {

	private final String[] predecessors = new String[]{};
	private final String study;

	public LeadTime(String study){
		this.study = study;
	}

	@Override
	public void execute(){
		var studyDocument = Mongo.findOne("Study", new Document("Name", study));
		var database = Settings.get("archiveDb", String.class);
		var name = this.getClass().getSimpleName();
		var pipeline = String.join("\n", Mongo.findOne("template.DrdlYaml", new Document("Type", "Degenerate").append("Name", name)).getList("Pipeline", String.class));
		var template = String.join("\n", Mongo.findOne("template.DrdlYaml", new Document("Type", "Degenerate").append("Name", name)).getList("Template", String.class));
		var forecastTime = Conversion.getForecastTime(studyDocument.getString("Time"));
		var eventTime = Conversion.getEventTime(studyDocument.getString("Time"));
		var format = Conversion.getMonthDateTimeFormatter();
		var forecastStartMonth = YearMonth.parse(studyDocument.getString("ForecastEndMonth").isEmpty() ? LocalDateTime.now().format(format) : studyDocument.getString("ForecastEndMonth"), format).plusMonths(-1).format(format);
		var forecastEndMonth = YearMonth.parse(studyDocument.getString("ForecastEndMonth").isEmpty() ? LocalDateTime.now().format(format) : studyDocument.getString("ForecastEndMonth"), format).plusMonths(1).format(format);

		var document = studyDocument.getList("Forecasts", String.class).stream().map(s -> new Document("Name", s)).reduce(new Document(), (d, s) -> {
			var forecastDocument = Mongo.findOne("Forecast", new Document("Name", s.getString("Name")));
			var collection = forecastDocument.getString("Collection");

			forecastDocument.getList("Filters", Document.class).forEach(f -> {
				var filter = f.get("Filter", Document.class).toJson();
				var t = pipeline.replace("{filter}", filter);
				t = t.replace("{forecastTime}", forecastTime);
				t = t.replace("{eventTime}", eventTime);
				t = t.replace("{forecastStartMonth}", forecastStartMonth);
				t = t.replace("{forecastEndMonth}", forecastEndMonth);
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
		IO.writeString(Path.of(Settings.get("drdlYamlPath"), String.format("%s_LeadTime.drdl.yml", study)), t);
	}

	@Override
	public String[] getPredecessors() {
		return predecessors;
	}
}
