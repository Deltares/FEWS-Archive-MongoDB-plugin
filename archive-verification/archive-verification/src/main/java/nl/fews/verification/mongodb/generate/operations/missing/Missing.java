package nl.fews.verification.mongodb.generate.operations.missing;

import nl.fews.verification.mongodb.generate.shared.conversion.Conversion;
import nl.fews.verification.mongodb.shared.database.Mongo;
import nl.fews.verification.mongodb.shared.mail.Mail;
import nl.fews.verification.mongodb.shared.settings.Settings;
import org.bson.Document;
import org.bson.json.JsonWriterSettings;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class Missing {

	private Missing(){}

	public static void execute() {
		var missing = StreamSupport.stream(Mongo.find("Study", new Document()).spliterator(), true).parallel().map(s -> {
			var study = new Document("Study", s.getString("Name"));

			var observed = Mongo.findOne("Observed", new Document("Name", s.getString("Observed")));
			var missingObserved = observed.getList("Filters", Document.class).parallelStream().map(l ->
				new Document("Observed", new Document("Name", observed.getString("Name")).append("FilterName", l.getString("FilterName")).append("Filter", l.get("Filter")).append("Collection", observed.getString("Collection")))).filter(l ->
					Mongo.aggregate(Settings.get("archiveDb"), observed.getString("Collection"), List.of(
						new Document("$match", l.get("Observed", Document.class).get("Filter", Document.class)),
						new Document("$match", new Document(Conversion.getEndTime(s.getString("Time")), new Document("$gte", Conversion.getYearMonthDate(s.getString("ForecastStartMonth"))))),
						new Document("$limit", 1))).first() == null).toList();
			if(!missingObserved.isEmpty())
				study.append("Observed", missingObserved);

			var normal = Mongo.findOne("Normal", new Document("Name", s.getString("Normal")));
			var missingNormal = normal.getList("Filters", Document.class).parallelStream().map(l ->
				new Document("Normal", new Document("Name", normal.getString("Name")).append("FilterName", l.getString("FilterName")).append("Filter", l.get("Filter")).append("Collection", normal.getString("Collection")))).filter(l ->
					Mongo.aggregate(Settings.get("archiveDb"), normal.getString("Collection"), List.of(
						new Document("$match", l.get("Normal", Document.class).get("Filter", Document.class)),
						new Document("$match", new Document(Conversion.getEndTime(s.getString("Time")), new Document("$gte", Conversion.getYearMonthDate(s.getString("ForecastStartMonth"))))),
						new Document("$limit", 1))).first() == null).toList();
			if(!missingNormal.isEmpty())
				study.append("Normal", missingNormal);

			var forecasts = StreamSupport.stream(Mongo.find("Forecast", new Document("Name", new Document("$in", s.getList("Forecasts", String.class)))).spliterator(), true);
			var missingForecast = forecasts.parallel().flatMap(forecast ->
				forecast.getList("Filters", Document.class).parallelStream().map(l ->
					new Document("Forecast", new Document("Name", forecast.getString("Name")).append("FilterName", l.getString("FilterName")).append("Filter", l.get("Filter")).append("Collection", forecast.getString("Collection")))).filter(l ->
						Mongo.aggregate(Settings.get("archiveDb"), forecast.getString("Collection"), List.of(
							new Document("$match", l.get("Forecast", Document.class).get("Filter", Document.class)),
							new Document("$match", new Document(Conversion.getForecastTime(s.getString("Time")), new Document("$gte", Conversion.getYearMonthDate(s.getString("ForecastStartMonth"))))),
							new Document("$limit", 1))).first() == null)).toList();
			if(!missingForecast.isEmpty())
				study.append("Forecasts", missingForecast);

			return study;

		}).filter(f -> f.size() > 1).toList();

		if(!missing.isEmpty())
			Mail.send("Missing Verification Data", String.format("[\n%s\n]", missing.stream().map(d -> d.toJson(JsonWriterSettings.builder().indent(true).build())).collect(Collectors.joining(",\n"))));
	}
}