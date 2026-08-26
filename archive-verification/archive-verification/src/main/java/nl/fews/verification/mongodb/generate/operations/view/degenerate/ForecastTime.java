package nl.fews.verification.mongodb.generate.operations.view.degenerate;

import nl.fews.verification.mongodb.generate.interfaces.IExecute;
import nl.fews.verification.mongodb.generate.interfaces.IPredecessor;
import nl.fews.verification.mongodb.generate.operations.view.Database;
import nl.fews.verification.mongodb.shared.database.Mongo;
import nl.fews.verification.mongodb.shared.settings.Settings;
import org.bson.Document;

public final class ForecastTime implements IExecute, IPredecessor {

	private final String[] predecessors = new String[]{};
	private final String study;

	public ForecastTime(String study){
		this.study = study;
	}

	@Override
	public void execute(){
		var name = this.getClass().getSimpleName();
		var database = Settings.get("verificationDb", String.class);
		var collection = String.format("verification.%s_ForecastObserved", study);
		var template = String.join("\n", Mongo.findOne("template.DrdlYaml", new Document("Type", "Degenerate").append("Name", name)).getList("Template", String.class));

		template = template.replace("{database}", database);
		template = template.replace("{study}", study);
		template = template.replace("{collection}", collection);
		
		Database.insertDrdlYamlToSqlView(template);
	}

	@Override
	public String[] getPredecessors() {
		return predecessors;
	}
}
