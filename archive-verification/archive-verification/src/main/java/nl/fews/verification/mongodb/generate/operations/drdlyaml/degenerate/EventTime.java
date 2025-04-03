package nl.fews.verification.mongodb.generate.operations.drdlyaml.degenerate;

import nl.fews.verification.mongodb.generate.interfaces.IExecute;
import nl.fews.verification.mongodb.generate.interfaces.IPredecessor;
import nl.fews.verification.mongodb.shared.database.Mongo;
import nl.fews.verification.mongodb.shared.io.IO;
import nl.fews.verification.mongodb.shared.settings.Settings;
import org.bson.Document;

import java.nio.file.Path;

public final class EventTime implements IExecute, IPredecessor {

	private final String[] predecessors = new String[]{};
	private final String study;

	public EventTime(String study){
		this.study = study;
	}

	@Override
	public void execute(){
		var collection = String.format("verification.%s_ForecastObserved", study);
		var database = Settings.get("archiveDb", String.class);
		var name = this.getClass().getSimpleName();
		var template = String.join("\n", Mongo.findOne("template.DrdlYaml", new Document("Type", "Degenerate").append("Name", name)).getList("Template", String.class));

		var t = template.replace("{database}", database);
		t = t.replace("{study}", study);
		t = t.replace("{collection}", collection);

		IO.writeString(Path.of(Settings.get("drdlYamlPath"), String.format("%s_%s.drdl.yml", study, name)), t);
	}

	@Override
	public String[] getPredecessors() {
		return predecessors;
	}
}
