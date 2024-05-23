package nl.fews.verification.mongodb.generate.operations.cube.tabular;

import nl.fews.verification.mongodb.generate.interfaces.IExecute;
import nl.fews.verification.mongodb.generate.interfaces.IPredecessor;
import nl.fews.verification.mongodb.generate.operations.cube.tabular.model.DataSources;
import nl.fews.verification.mongodb.generate.operations.cube.tabular.model.Roles;
import nl.fews.verification.mongodb.generate.operations.cube.tabular.model.Tables;
import nl.fews.verification.mongodb.shared.database.Mongo;
import nl.fews.verification.mongodb.shared.io.IO;
import nl.fews.verification.mongodb.shared.settings.Settings;
import org.bson.Document;
import org.bson.json.JsonWriterSettings;

import java.nio.file.Path;

public final class Model implements IExecute, IPredecessor {

	private final String[] predecessors = new String[]{};
	private final String study;

	public Model(String study){
		this.study = study;
	}

	@Override
	public void execute(){
		var studyDocument = Mongo.findOne("Study", new Document("Name", study));
		var cube = studyDocument.getString("Cube");
		var template = Mongo.findOne("template.Cube", new Document("Name", cube)).get("Template", Document.class);

		template.append("id", String.format("Verification_%s", study));
		template.append("name", String.format("Verification_%s", study));
		template.get("model", Document.class).append("name", String.format("Verification_%s", study));

		new DataSources(template).generate();
		new Tables(studyDocument, template).generate();
		new Roles(template).generate();

		IO.writeString(Path.of(Settings.get("bimPath"), String.format("Verification_%s.bim", study)), template.toJson(JsonWriterSettings.builder().indent(true).build()));
	}

	@Override
	public String[] getPredecessors() {
		return predecessors;
	}
}
