package nl.fews.verification.mongodb.generate.operations.powerquery.expression;

import nl.fews.verification.mongodb.generate.interfaces.IExecute;
import nl.fews.verification.mongodb.generate.interfaces.IPredecessor;
import nl.fews.verification.mongodb.generate.shared.conversion.Conversion;
import nl.fews.verification.mongodb.shared.database.Mongo;
import nl.fews.verification.mongodb.shared.settings.Settings;
import org.bson.Document;

import java.time.LocalDateTime;
import java.time.YearMonth;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.stream.StreamSupport;

public final class ForecastObserved implements IExecute, IPredecessor {

	private final String[] predecessors = new String[]{};
	private final String study;

	public ForecastObserved(String study){
		this.study = study;
	}

	@Override
	public void execute(){
		var studyDocument = Mongo.findOne("Study", new Document("Name", study));
		var forecastStartMonth = studyDocument.getString("ForecastStartMonth");
		var format = Conversion.getMonthDateTimeFormatter();
		var name = this.getClass().getSimpleName();
		var databaseConnectionString = Settings.get("databaseConnectionString", String.class);
		var template = String.join("\n", Mongo.findOne("template.PowerQuery", new Document("Name", name)).getList("Template", String.class));
		var database = Settings.get("archiveDb", String.class);

		var forecasts = Mongo.find("Forecast", new Document("Name", new Document("$in", studyDocument.getList("Forecasts", String.class))));
		var toDeduplicate = StreamSupport.stream(forecasts.spliterator(), false).anyMatch(f -> f.getList("Filters", Document.class).stream().anyMatch(t -> t.getBoolean("Deduplicate")));
		var deduplicate = toDeduplicate ?
			"Table.Distinct(Table.Sort(Forecasts, {\"forecastId\", \"location\", \"forecastTime\", \"eventTime\", {\"partitionTime\", Order.Descending}, {\"dispatchTime\": Order.Descending}}), {\"forecastId\", \"location\", \"forecastTime\", \"eventTime\"})" :
			"Forecasts";

		var startMonth = YearMonth.parse(forecastStartMonth);
		var endMonth = YearMonth.parse(studyDocument.getString("ForecastEndMonth").isEmpty() ? LocalDateTime.now().plusDays(1).format(format) : studyDocument.getString("ForecastEndMonth"), format);

		var months = new ArrayList<YearMonth>();
		for(var m = startMonth; m.compareTo(endMonth) <= 0; m = m.plusMonths(1))
			months.add(m);

		months.parallelStream().forEach(m -> {
			var month = m.format(format);

			var t = template.replace("{databaseConnectionString}", databaseConnectionString);
			t = t.replace("{database}", database);
			t = t.replace("{deduplicate}", deduplicate);
			t = t.replace("{study}", study);
			t = t.replace("{month}", month);
			t = t.replace("{endMonth}", m.plusMonths(1).format(format));

			Mongo.insertOne("output.PowerQuery", new Document("Study", study).append("Name", name).append("Month", month).append("Expression", Arrays.stream(t.replace("\r", "").split("\n")).toList()));
		});
	}

	@Override
	public String[] getPredecessors() {
		return predecessors;
	}
}
