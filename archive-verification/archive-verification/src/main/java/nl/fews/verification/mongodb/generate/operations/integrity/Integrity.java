package nl.fews.verification.mongodb.generate.operations.integrity;

import nl.fews.verification.mongodb.shared.database.Mongo;
import nl.fews.verification.mongodb.shared.mail.Mail;
import org.bson.Document;
import org.bson.json.JsonWriterSettings;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class Integrity {

	private Integrity(){}

	public static void execute() {
		missing();
		orphaned();
	}

	private static void orphaned() {
		var orphaned = Stream.of("Observed", "Normal", "Class", "LocationAttributes").map(fieldCollection -> {
			var existing = StreamSupport.stream(Mongo.find("Study", new Document()).spliterator(), true).parallel().map(s -> s.getString(fieldCollection)).collect(Collectors.toSet());
			return Map.entry(fieldCollection, StreamSupport.stream(Mongo.find(fieldCollection, new Document()).spliterator(), true).filter(d -> !existing.contains(d.getString("Name"))).map(d -> d.getString("Name")).collect(Collectors.toList()));
		}).filter(o -> !o.getValue().isEmpty()).collect(Collectors.toList());

		orphaned.addAll(Map.of("Forecasts", "Forecast", "Seasonalities", "Seasonality").entrySet().stream().map(f -> {
			var field = f.getKey();
			var collection = f.getValue();
			var existing = StreamSupport.stream(Mongo.find("Study", new Document()).spliterator(), true).parallel().flatMap(t -> t.getList(field, String.class).stream()).collect(Collectors.toSet());
			return Map.entry(collection, StreamSupport.stream(Mongo.find(collection, new Document()).spliterator(), true).filter(d -> !existing.contains(d.getString("Name"))).map(d -> d.getString("Name")).collect(Collectors.toList()));
		}).filter(o -> !o.getValue().isEmpty()).collect(Collectors.toList()));

		if(!orphaned.isEmpty())
			Mail.send("Orphaned Collection Entries", String.format("%s", new Document(orphaned.stream().collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))).toJson(JsonWriterSettings.builder().indent(true).build())));
	}

	private static void missing(){
		var missing = StreamSupport.stream(Mongo.find("Study", new Document()).spliterator(), true).parallel().map(s -> {
			var study = new Document("Study", s.getString("Name"));

			List.of("Observed", "Normal", "Class", "LocationAttributes").forEach(f -> {
				if(Mongo.findOne(f, new Document("Name", s.getString(f))) == null)
					study.append(f, s.getString(f));
			});

			Map.of("Forecasts", "Forecast", "Seasonalities", "Seasonality").forEach((field, collection) -> {
				var existing = StreamSupport.stream(Mongo.find(collection, new Document("Name", new Document("$in", s.getList(field, String.class)))).spliterator(), true).map(t -> t.getString("Name")).collect(Collectors.toSet());
				var m = s.getList(field, String.class).stream().filter(n -> !existing.contains(n)).collect(Collectors.toList());
				if(!m.isEmpty())
					study.append(field, m);
			});
			return study;

		}).filter(f -> f.size() > 1).collect(Collectors.toList());

		if(!missing.isEmpty())
			Mail.send("Missing Study Lookups", String.format("[\n%s\n]", missing.stream().map(d -> d.toJson(JsonWriterSettings.builder().indent(true).build())).collect(Collectors.joining(",\n"))));
	}
}