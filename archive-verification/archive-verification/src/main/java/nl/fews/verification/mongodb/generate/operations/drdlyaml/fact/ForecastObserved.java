package nl.fews.verification.mongodb.generate.operations.drdlyaml.fact;

import nl.fews.verification.mongodb.generate.interfaces.IExecute;
import nl.fews.verification.mongodb.generate.interfaces.IPredecessor;
import nl.fews.verification.mongodb.shared.database.Mongo;
import nl.fews.verification.mongodb.shared.io.IO;
import nl.fews.verification.mongodb.shared.settings.Settings;
import org.bson.Document;

import java.nio.file.Path;

public final class ForecastObserved implements IExecute, IPredecessor {

	private final String[] predecessors = new String[]{};
	private final String study;

	public ForecastObserved(String study){
		this.study = study;
	}

	@Override
	public void execute(){
		var name = this.getClass().getSimpleName();
		var environment = Settings.get("environment", String.class);
		var database = Settings.get("archiveDb", String.class);
		var template = String.join("\n", Mongo.findOne("template.DrdlYaml", new Document("Type", "Fact").append("Name", name)).getList("Template", String.class));

		Mongo.listCollections(database).filter(new Document("type", "view").append("name", new Document("$regex", String.format("^view\\.verification\\.%s\\.%s|Forecast|", environment, study)))).forEach(m -> {
			var month = m.getString("name").substring(m.getString("name").lastIndexOf('|')+1);
			var t = template.replace("{database}", database);
			t = t.replace("{environment}",  environment);
			t = t.replace("{month}", month);
			t = t.replace("{study}", study);

			IO.writeString(Path.of(Settings.get("drdlYamlPath"), String.format("%s_%s.drdl.yml", study, month)), t);
		});
	}

	@Override
	public String[] getPredecessors() {
		return predecessors;
	}
}
