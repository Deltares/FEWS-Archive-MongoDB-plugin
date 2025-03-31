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
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

@SuppressWarnings("unchecked")
public final class Location implements IExecute, IPredecessor {

	private final String[] predecessors = new String[]{};
	private final String study;

	public Location(String study){
		this.study = study;
	}

	@Override
	public void execute(){
		var name = this.getClass().getSimpleName();
		var dataPath = Path.of(Settings.get("csvPath", String.class), "data", study, name);
		if (!Files.exists(dataPath))
			IO.createDirectories(dataPath);
		IO.deleteTree(dataPath);

		var verificationDatabase = Settings.get("verificationDb", String.class);
		var archiveDatabase = Settings.get("archiveDb", String.class);

		var aData = query(name, archiveDatabase).stream().collect(Collectors.toMap(d -> d.getString("locationId"), d -> d, (a, b) -> b));
		var vData = query(name, verificationDatabase).stream().collect(Collectors.toMap(d -> d.getString("locationId"), d -> d, (a, b) -> b));
		var columns = new ArrayList<String>();
		var lines = aData.entrySet().stream().filter(a -> vData.containsKey(a.getKey())).sorted(Comparator.comparing(s -> s.getValue().getString("location"))).map(a -> {
			if (columns.isEmpty()){
				columns.add("location");
				columns.addAll(vData.get(a.getKey()).keySet().stream().toList());
			}
			var values = vData.get(a.getKey()).values().stream().map(Object::toString).collect(Collectors.toList());
			values.add(0, a.getValue().getString("location"));
			return String.join("\t", values);
		}).collect(Collectors.toList());
		lines.add(0, String.join("\t", columns));

		IO.writeString(Path.of(dataPath.toString(), String.format("%s_%s.csv", study, name)), String.join("\n", lines));
	}

	private List<Document> query(String name, String database){
		var drdlYaml = (Document) Conversion.toBson(new Yaml().load(String.join("\n", Mongo.findOne("output.DrdlYaml", new Document("Study", study).append("Name", String.format("%s_%s_%s", study, name, database))).getList("Expression", String.class))));
		var db = drdlYaml.getList("schema", Document.class).get(0).getString("db");
		var collection = drdlYaml.getList("schema", Document.class).get(0).getList("tables", Document.class).get(0).getString("collection");
		var pipeline = drdlYaml.getList("schema", Document.class).get(0).getList("tables", Document.class).get(0).getList("pipeline", Document.class);

		var lines= new ArrayList<Document>();
		Mongo.aggregate(db, collection, (List<Document>) Conversion.convertDate(pipeline)).forEach(lines::add);
		return lines;
	}

	@Override
	public String[] getPredecessors() {
		return predecessors;
	}
}
