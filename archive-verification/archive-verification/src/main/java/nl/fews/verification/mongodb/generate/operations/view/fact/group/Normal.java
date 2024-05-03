package nl.fews.verification.mongodb.generate.operations.view.fact.group;

import nl.fews.verification.mongodb.generate.interfaces.IExecute;
import nl.fews.verification.mongodb.generate.interfaces.IPredecessor;
import nl.fews.verification.mongodb.generate.shared.conversion.Conversion;
import nl.fews.verification.mongodb.shared.database.Mongo;
import nl.fews.verification.mongodb.shared.settings.Settings;
import org.bson.Document;

import java.time.LocalDateTime;
import java.time.YearMonth;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public final class Normal implements IExecute, IPredecessor {

	private final String[] predecessors = new String[]{};
	private final String study;

	public Normal(String study){
		this.study = study;
	}

	@Override
	public void execute(){
		var studyDocument = Mongo.findOne("Study", new Document("Name", study));
		var forecastStartMonth = studyDocument.getString("ForecastStartMonth");
		var timeMatch = new Document(Conversion.getEndTime(studyDocument.getString("Time")), new Document("$gte", new Document("$date", Conversion.getYearMonthDate(forecastStartMonth))));
		var environment = Settings.get("environment", String.class);
		var database = Settings.get("archiveDb", String.class);
		var name = this.getClass().getSimpleName();
		var existingCurrent = StreamSupport.stream(Mongo.find("output.View", new Document("State", "current").append("Name", name).append("Environment", environment).append("Study", study)).spliterator(), false).collect(Collectors.toMap(f -> f.getString("View"), f -> f));
		var existing = StreamSupport.stream(Mongo.listCollections(Settings.get("archiveDb")).filter(new Document("type", "view").append("name", new Document("$regex", String.format("^view\\.verification\\.%s\\.%s\\|%s\\|\\d{4}-\\d{2}$", environment, study, name)))).spliterator(), false).map(s -> s.getString("name")).collect(Collectors.toSet());

		var format = Conversion.getMonthDateTimeFormatter();

		var normalDocument = Mongo.findOne("Normal", new Document("Name", studyDocument.getString("Normal")));
		var min = normalDocument.getList("Filters", Document.class).parallelStream().map(l -> {
			var filter = l.get("Filter", Document.class);
			var m = Mongo.aggregate(database, normalDocument.getString("Collection"), List.of(new Document("$match", filter), new Document("$match", timeMatch), new Document("$group", new Document("_id", null).append("min", new Document("$min", String.format("$%s", Conversion.getStartTime(studyDocument.getString("Time")))))))).first();
			return m == null ? new Date(Long.MAX_VALUE) : m.getDate("min");
		}).min(Date::compareTo).orElse(new Date(Long.MAX_VALUE));

		if(!min.equals(new Date(Long.MAX_VALUE))) {
			var startMonth = Conversion.max(YearMonth.parse(forecastStartMonth), Conversion.getYearMonth(min));
			var endMonth = YearMonth.parse(studyDocument.getString("ForecastEndMonth").isEmpty() ? LocalDateTime.now().plusDays(1).format(format) : studyDocument.getString("ForecastEndMonth"), format);
			var template = String.join("\n", Mongo.findOne("template.View", new Document("Type", "FactGroup").append("Name", name)).getList("Template", String.class));

			var months = new ArrayList<YearMonth>();
			for (var m = startMonth; m.compareTo(endMonth) <= 0; m = m.plusMonths(1))
				months.add(m);

			var created = months.parallelStream().map(m -> {
				var union = normalDocument.getList("Filters", Document.class).stream().map(f -> String.format("view.verification.%s.%s|%s|%s|%s", environment, study, name, f.getString("FilterName"), m.format(format))).collect(Collectors.toList());
				var collection = union.get(0);
				var view = String.format("view.verification.%s.%s|%s|%s", environment, study, name, m.format(format));
				var t = template.replace("{union}", union.size() > 1 ? union.subList(1, union.size()).stream().map(s -> String.format("{\"$unionWith\": \"%s\"},", s)).collect(Collectors.joining("\n")) : "");
				var document = Document.parse(String.format("{\"document\":[%s]}", t));
				if (!existing.contains(view)) {
					Mongo.createView(database, view, collection, document.getList("document", Document.class));
				}
				else if(existingCurrent.containsKey(view) && (!existingCurrent.get(view).get("Value").equals(document) || !existingCurrent.get(view).get("Collection").equals(collection))) {
					Mongo.dropCollection(database, view);
					Mongo.createView(database, view, collection, document.getList("document", Document.class));
				}
				Mongo.insertOne("output.View", new Document("Database", database).append("State", "new").append("View", view).append("Collection", collection).append("Name", name).append("Environment", environment).append("Study", study).append("Value", document));
				return view;
			}).collect(Collectors.toSet());
			existing.parallelStream().filter(e -> !created.contains(e)).forEach(d -> Mongo.dropCollection(database, d));
		}
		else
		{
			existing.parallelStream().forEach(d -> Mongo.dropCollection(database, d));
		}
	}

	@Override
	public String[] getPredecessors() {
		return predecessors;
	}
}
