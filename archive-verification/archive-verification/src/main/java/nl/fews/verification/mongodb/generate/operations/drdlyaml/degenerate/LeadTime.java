package nl.fews.verification.mongodb.generate.operations.drdlyaml.degenerate;

import nl.fews.verification.mongodb.generate.interfaces.IExecute;
import nl.fews.verification.mongodb.generate.interfaces.IPredecessor;
import nl.fews.verification.mongodb.shared.database.Mongo;
import nl.fews.verification.mongodb.shared.io.IO;
import nl.fews.verification.mongodb.shared.settings.Settings;
import org.bson.Document;

import java.nio.file.Path;

public final class LeadTime implements IExecute, IPredecessor {

	private final String[] predecessors = new String[]{};
	private final String study;

	public LeadTime(String study){
		this.study = study;
	}

	@Override
	public void execute(){
		var name = this.getClass().getSimpleName();
		var database = Settings.get("verificationDb", String.class);
		var collection = String.format("verification.%s_ForecastObserved", study);
		var template = String.join("\n", Mongo.findOne("template.DrdlYaml", new Document("Type", "Degenerate").append("Name", name)).getList("Template", String.class));

		var studyDocument = Mongo.findOne("Study", new Document("Name", study));
		var maxLeadTimeMinutes = studyDocument.getInteger("MaxLeadTimeMinutes").toString();

		template = template.replace("{database}", database);
		template = template.replace("{study}", study);
		template = template.replace("{collection}", collection);
		template = template.replace("{maxLeadTimeMinutes}", maxLeadTimeMinutes);
		
		IO.writeString(Path.of(Settings.get("drdlYamlPath"), String.format("%s_%s.drdl.yml", study, name)), template);
	}

	@Override
	public String[] getPredecessors() {
		return predecessors;
	}
}
