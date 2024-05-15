package nl.fews.verification.mongodb.generate.operations.drdlyaml.dimension;

import nl.fews.verification.mongodb.generate.interfaces.IExecute;
import nl.fews.verification.mongodb.generate.interfaces.IPredecessor;
import nl.fews.verification.mongodb.shared.database.Mongo;
import nl.fews.verification.mongodb.shared.io.IO;
import nl.fews.verification.mongodb.shared.settings.Settings;
import org.bson.Document;

import java.nio.file.Path;

public final class Measure implements IExecute, IPredecessor {

	private final String[] predecessors = new String[]{};
	private final String study;

	public Measure(String study){
		this.study = study;
	}

	@Override
	public void execute(){
		var environment = Settings.get("environment", String.class);
		var template = String.join("\n", Mongo.findOne("template.DrdlYaml", new Document("Type", "Dimension").append("Name", "Measure")).getList("Template", String.class));
		template = template.replace("{database}", Settings.get("verificationDb"));
		template = template.replace("{environment}",  environment);
		template = template.replace("{study}", study);
		IO.writeString(Path.of(Settings.get("drdlYamlPath"), String.format("%s_Measure.drdl.yml", study)), template);
	}

	@Override
	public String[] getPredecessors() {
		return predecessors;
	}
}
