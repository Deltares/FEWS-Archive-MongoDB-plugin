package nl.fews.verification.mongodb.generate.operations.deploy.cube;

import nl.fews.verification.mongodb.generate.interfaces.IExecute;
import nl.fews.verification.mongodb.generate.interfaces.IPredecessor;
import nl.fews.verification.mongodb.shared.database.Mongo;
import nl.fews.verification.mongodb.shared.io.IO;
import nl.fews.verification.mongodb.shared.settings.Settings;
import org.bson.Document;
import org.bson.json.JsonWriterSettings;

import java.nio.file.Path;

public final class Cube implements IExecute, IPredecessor {

	private final String[] predecessors = new String[]{};
	private final String study;

	public Cube(String study){
		this.study = study;
	}

	@Override
	public void execute(){
		IO.writeString(Path.of(Settings.get("bimPath"), String.format("Verification_%s.bim", study)), Mongo.findOne("output.Cube", new Document("Name", study)).get("Bim", Document.class).toJson(JsonWriterSettings.builder().indent(true).build()));
	}

	@Override
	public String[] getPredecessors() {
		return predecessors;
	}
}
