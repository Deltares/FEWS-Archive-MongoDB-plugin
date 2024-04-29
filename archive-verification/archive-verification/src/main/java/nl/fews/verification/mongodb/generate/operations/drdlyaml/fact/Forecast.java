package nl.fews.verification.mongodb.generate.operations.drdlyaml.fact;

import nl.fews.verification.mongodb.generate.interfaces.IExecute;
import nl.fews.verification.mongodb.generate.interfaces.IPredecessor;
import nl.fews.verification.mongodb.generate.shared.conversion.Conversion;
import nl.fews.verification.mongodb.shared.database.Mongo;
import nl.fews.verification.mongodb.shared.io.IO;
import nl.fews.verification.mongodb.shared.settings.Settings;
import org.bson.Document;

import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.YearMonth;

public final class Forecast implements IExecute, IPredecessor {

	private final String[] predecessors = new String[]{};
	private final String study;

	public Forecast(String study){
		this.study = study;
	}

	@Override
	public void execute(){
		var name = this.getClass().getSimpleName();
		var studyDocument = Mongo.findOne("Study", new Document("Name", study));
		var environment = Settings.get("environment", String.class);
		var format = Conversion.getMonthDateTimeFormatter();
		var endMonth = YearMonth.parse(studyDocument.getString("ForecastEndMonth").isEmpty() ? LocalDateTime.now().plusDays(1).format(format) : studyDocument.getString("ForecastEndMonth"), format);
		var template = String.join("\n", Mongo.findOne("template.DrdlYaml", new Document("Type", "Fact").append("Name", name)).getList("Template", String.class));
		for(var m = YearMonth.parse(studyDocument.getString("ForecastStartMonth")); m.compareTo(endMonth) <= 0; m = m.plusMonths(1)) {
			var t = template.replace("{database}", Settings.get("archiveDb"));
			t = t.replace("{environment}",  environment);
			t = t.replace("{month}", m.format(format));
			t = t.replace("{study}", study);

			IO.writeString(Path.of(Settings.get("drdlYamlPath"), String.format("%s_%s_%s.drdl.yml", study, name, m.format(format))), t);
		}
	}

	@Override
	public String[] getPredecessors() {
		return predecessors;
	}
}
