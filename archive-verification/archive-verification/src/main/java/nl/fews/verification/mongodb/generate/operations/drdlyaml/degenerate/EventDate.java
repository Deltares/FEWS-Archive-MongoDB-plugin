package nl.fews.verification.mongodb.generate.operations.drdlyaml.degenerate;

import nl.fews.verification.mongodb.generate.interfaces.IExecute;
import nl.fews.verification.mongodb.generate.interfaces.IPredecessor;
import nl.fews.verification.mongodb.generate.shared.conversion.Conversion;
import nl.fews.verification.mongodb.shared.database.Mongo;
import nl.fews.verification.mongodb.shared.io.IO;
import nl.fews.verification.mongodb.shared.settings.Settings;
import org.bson.Document;

import java.nio.file.Path;
import java.util.stream.StreamSupport;

public final class EventDate implements IExecute, IPredecessor {

	private final String[] predecessors = new String[]{};
	private final String study;

	public EventDate(String study){
		this.study = study;
	}

	@Override
	public void execute(){
		var collection = String.format("verification.%s_ForecastObserved", study);
		var studyDocument = Mongo.findOne("Study", new Document("Name", study));
		var database = Settings.get("archiveDb", String.class);
		var name = this.getClass().getSimpleName();
		var seasonalityColumns = Conversion.getSeasonalityColumns(studyDocument.getList("Seasonalities", String.class));
		var template = String.join("\n", Mongo.findOne("template.DrdlYaml", new Document("Type", "Degenerate").append("Name", name)).getList("Template", String.class));
		var seasonalities = Conversion.getSeasonalities(StreamSupport.stream(Mongo.find("Seasonality", new Document("Name", new Document("$in", studyDocument.getList("Seasonalities", String.class)))).spliterator(), false).toList());

		var t = template.replace("{database}", database);
		t = t.replace("{study}", study);
		t = t.replace("{collection}", collection);
		t = t.replace("{seasonalities}", seasonalities);
		t = t.replace("{seasonalityColumns}", seasonalityColumns);

		IO.writeString(Path.of(Settings.get("drdlYamlPath"), String.format("%s_%s.drdl.yml", study, name)), t);
	}

	@Override
	public String[] getPredecessors() {
		return predecessors;
	}
}