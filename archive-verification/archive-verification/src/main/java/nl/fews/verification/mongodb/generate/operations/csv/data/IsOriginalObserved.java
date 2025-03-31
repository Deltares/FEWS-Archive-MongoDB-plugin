package nl.fews.verification.mongodb.generate.operations.csv.data;

import nl.fews.verification.mongodb.generate.interfaces.IExecute;
import nl.fews.verification.mongodb.generate.interfaces.IPredecessor;
import nl.fews.verification.mongodb.generate.shared.conversion.Conversion;
import nl.fews.verification.mongodb.shared.database.Mongo;
import nl.fews.verification.mongodb.shared.io.IO;
import nl.fews.verification.mongodb.shared.settings.Settings;
import org.bson.Document;
import org.yaml.snakeyaml.Yaml;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;

public final class IsOriginalObserved implements IExecute, IPredecessor {

	private final String[] predecessors = new String[]{};
	private final String study;

	public IsOriginalObserved(String study){
		this.study = study;
	}

	@Override
	public void execute(){
		var name = this.getClass().getSimpleName();
		var dataPath = Path.of(Settings.get("csvPath", String.class), "data", study, name);
		if (!Files.exists(dataPath))
			IO.createDirectories(dataPath);
		IO.deleteTree(dataPath);

		var drdlYaml = (Document) Conversion.toBson(new Yaml().load(String.join("\n", Mongo.findOne("output.DrdlYaml", new Document("Study", study).append("Name", String.format("%s_%s", study, name))).getList("Expression", String.class))));
		var db = drdlYaml.getList("schema", Document.class).get(0).getString("db");
		var collection = drdlYaml.getList("schema", Document.class).get(0).getList("tables", Document.class).get(0).getString("collection");
		var pipeline = drdlYaml.getList("schema", Document.class).get(0).getList("tables", Document.class).get(0).getList("pipeline", Document.class);

		var lines= new ArrayList<String>();
		Mongo.aggregate(db, collection, pipeline).forEach(r -> {
			if (lines.isEmpty())
				lines.add(String.join("\t", r.keySet()));
			lines.add(String.join("\t", r.values().stream().map(Object::toString).toList()));
		});
		IO.writeString(Path.of(dataPath.toString(), String.format("%s_%s.csv", study, name)), String.join("\n", lines));
	}

	@Override
	public String[] getPredecessors() {
		return predecessors;
	}
}
