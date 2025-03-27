package nl.fews.verification.mongodb.generate.operations.drdlyaml.dimension;

import nl.fews.verification.mongodb.generate.interfaces.IExecute;
import nl.fews.verification.mongodb.generate.interfaces.IPredecessor;
import nl.fews.verification.mongodb.shared.database.Mongo;
import nl.fews.verification.mongodb.shared.io.IO;
import nl.fews.verification.mongodb.shared.settings.Settings;
import org.bson.Document;

import java.nio.file.Path;
import java.util.Arrays;

public final class Measure implements IExecute, IPredecessor {

	private final String[] predecessors = new String[]{};
	private final String study;

	public Measure(String study){
		this.study = study;
	}

	@Override
	public void execute(){
		var studyDocument = Mongo.findOne("Study", new Document("Name", study));
		var name = this.getClass().getSimpleName();
		var template = String.join("\n", Mongo.findOne("template.DrdlYaml", new Document("Type", "Dimension").append("Name", name)).getList("Template", String.class));
		template = template.replace("{database}", Settings.get("verificationDb"));
		template = template.replace("{study}", study);
		
		if(studyDocument.getString("Cube").equals("Default"))
			IO.writeString(Path.of(Settings.get("drdlYamlPath"), String.format("%s_%s.drdl.yml", study, name)), template);
		else if (studyDocument.getString("Cube").equals("Csv"))
			Mongo.insertOne("output.DrdlYaml", new Document("Study", study).append("Name", String.format("%s_%s", study, name)).append("Expression", Arrays.stream(template.replace("\r", "").split("\n")).toList()));
	}

	@Override
	public String[] getPredecessors() {
		return predecessors;
	}
}
