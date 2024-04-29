package nl.fews.verification.mongodb.generate.operations.drdlyaml.degenerate;

import nl.fews.verification.mongodb.generate.interfaces.IExecute;
import nl.fews.verification.mongodb.generate.interfaces.IPredecessor;
import nl.fews.verification.mongodb.generate.shared.conversion.Conversion;
import nl.fews.verification.mongodb.shared.database.Mongo;
import nl.fews.verification.mongodb.shared.io.IO;
import nl.fews.verification.mongodb.shared.settings.Settings;
import org.bson.Document;

import java.nio.file.Path;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public final class ForecastDate implements IExecute, IPredecessor {

	private final String[] predecessors = new String[]{};
	private final String study;

	public ForecastDate(String study){
		this.study = study;
	}

	@Override
	public void execute(){
		var studyDocument = Mongo.findOne("Study", new Document("Name", study));
		var environment = Settings.get("environment", String.class);
		var seasonalityColumns = Conversion.getSeasonalityColumns(studyDocument.getList("Seasonalities", String.class));

		var union = StreamSupport.stream(Mongo.find("Forecast", new Document("Name", new Document("$in", studyDocument.getList("Forecasts", String.class)))).spliterator(), false).flatMap(s ->
			s.getList("Filters", Document.class).stream().map(f -> String.format("view.verification.%s.%s|%s|ForecastDate|%s", environment, study, s.getString("ForecastName"), f.getString("FilterName")))).collect(Collectors.toList());

		var template = String.join("\n", Mongo.findOne("template.DrdlYaml", new Document("Type", "Degenerate").append("Name", "ForecastDate")).getList("Template", String.class));
		template = template.replace("{database}", Settings.get("archiveDb"));
		template = template.replace("{table}", String.format("%s_ForecastDate", study));
		template = template.replace("{collection}", union.get(0));
		template = template.replace("{union}", union.size() > 1 ? union.subList(1, union.size()).stream().map(s -> String.format("      {\"$unionWith\": \"%s\"},", s)).collect(Collectors.joining("\n")) : "");
		template = template.replace("{seasonalityColumns}", seasonalityColumns);

		IO.writeString(Path.of(Settings.get("drdlYamlPath"), String.format("%s_ForecastDate.drdl.yml", study)), template);
	}

	@Override
	public String[] getPredecessors() {
		return predecessors;
	}
}
