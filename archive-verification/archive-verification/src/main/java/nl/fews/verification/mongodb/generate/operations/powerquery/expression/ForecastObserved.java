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

		var deduplicate = StreamSupport.stream(Mongo.find("Forecast", new Document("Name", new Document("$in", studyDocument.getList("Forecasts", String.class)))).spliterator(), false).anyMatch(f -> f.getList("Filters", Document.class).stream().anyMatch(t -> t.getBoolean("Deduplicate")));
		var forecasts = deduplicate ?
			"Table.Sort(Table.RemoveColumns(Table.Distinct(Odbc.Query(Source, \"SELECT forecastName, forecastId, location, ensemble, ensembleMember, forecastTime, forecastDate, forecastMinute, isOriginalForecast, eventTime, eventDate, eventMinute, leadTime, forecast, forecastClass, partitionTime, dispatchTime FROM {database}.`{study}_Forecasts` WHERE partitionTime >= '{month}-01' AND partitionTime < '{endMonth}-01' ORDER BY forecastId, location, forecastTime, eventTime, partitionTime DESC, dispatchTime DESC\"), {\"forecastId\", \"location\", \"forecastTime\", \"eventTime\"}), {\"partitionTime\", \"dispatchTime\"}), {\"location\", \"forecastTime\"})" :
			"Odbc.Query(Source, \"SELECT forecastName, forecastId, location, ensemble, ensembleMember, forecastTime, forecastDate, forecastMinute, isOriginalForecast, eventTime, eventDate, eventMinute, leadTime, forecast, forecastClass FROM {database}.`{study}_Forecasts` WHERE partitionTime >= '{month}-01' AND partitionTime < '{endMonth}-01' ORDER BY location, forecastTime\")";

		var startMonth = YearMonth.parse(forecastStartMonth);
		var endMonth = YearMonth.parse(studyDocument.getString("ForecastEndMonth").isEmpty() ? LocalDateTime.now().plusDays(1).format(format) : studyDocument.getString("ForecastEndMonth"), format);

		var months = new ArrayList<YearMonth>();
		for(var m = startMonth; m.compareTo(endMonth) <= 0; m = m.plusMonths(1))
			months.add(m);

		months.parallelStream().forEach(m -> {
			var month = m.format(format);

			var t = template.replace("{databaseConnectionString}", databaseConnectionString);
			t = t.replace("{forecasts}", forecasts);
			t = t.replace("{database}", database);
			t = t.replace("{study}", study);
			t = t.replace("{month}", month);
			t = t.replace("{endMonth}", m.plusMonths(1).format(format));
			t = t.replace("{observedEndMonth}", m.plusMonths(2).format(format));

			Mongo.insertOne("output.PowerQuery", new Document("Study", study).append("Name", name).append("Month", month).append("Expression", Arrays.stream(t.replace("\r", "").split("\n")).toList()));
		});
	}

	@Override
	public String[] getPredecessors() {
		return predecessors;
	}
}
