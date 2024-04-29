package nl.fews.verification.mongodb.generate.operations.view.fact;

import nl.fews.verification.mongodb.generate.interfaces.IExecute;
import nl.fews.verification.mongodb.generate.interfaces.IPredecessor;
import nl.fews.verification.mongodb.generate.shared.conversion.Conversion;
import nl.fews.verification.mongodb.shared.database.Mongo;
import nl.fews.verification.mongodb.shared.settings.Settings;
import org.bson.Document;

import java.time.LocalDateTime;
import java.time.YearMonth;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public final class Observed implements IExecute, IPredecessor {

	private final String[] predecessors = new String[]{};
	private final String study;

	public Observed(String study){
		this.study = study;
	}

	@Override
	public void execute(){
		var studyDocument = Mongo.findOne("Study", new Document("Name", study));
		var environment = Settings.get("environment", String.class);
		var database = Settings.get("archiveDb", String.class);
		var name = this.getClass().getSimpleName();
		var observedDocument = Mongo.findOne("Observed", new Document("Name", studyDocument.getString("Observed")));
		var collection = observedDocument.getString("Collection");
		var existingCurrent = StreamSupport.stream(Mongo.find("output.View", new Document("State", "current").append("Name", name).append("Environment", environment).append("Study", study)).spliterator(), false).collect(Collectors.toMap(f -> f.getString("View"), f -> f));
		var existing = StreamSupport.stream(Mongo.listCollections(database).filter(new Document("type", "view").append("name", new Document("$regex", String.format("^view\\.verification\\.%s\\.%s\\|%s\\|.+\\|\\d{4}-\\d{2}$", environment, study, name)))).spliterator(), false).map(s -> s.getString("name")).collect(Collectors.toSet());
		var template = String.join("\n", Mongo.findOne("template.View", new Document("Type", "Fact").append("Name", name)).getList("Template", String.class));

		var created = observedDocument.getList("Filters", Document.class).parallelStream().flatMap(f -> {
			var filter = f.get("Filter", Document.class);
			var filterName = f.getString("FilterName");
			var startTime = Conversion.getStartTime(studyDocument.getString("Time"));
			var endTime = Conversion.getEndTime(studyDocument.getString("Time"));
			var eventTime = Conversion.getEventTime(studyDocument.getString("Time"));
			var eventValue= Conversion.getEventValue(studyDocument.getString("Value"));
			var locationMap = Conversion.getLocationMap(f.get("LocationMap", Document.class));
			var observedClass = Conversion.getObservedClass(studyDocument.getString("Class").isEmpty() ? null : Mongo.findOne("Class", new Document("Name", studyDocument.getString("Class"))));

			var format = Conversion.getMonthDateTimeFormatter();
			var min = Mongo.aggregate(database, collection, List.of(new Document("$match", filter), new Document("$group", new Document("_id", null).append("min", new Document("$min", String.format("$%s", startTime)))))).first();
			if(min != null) {
				var startMonth = Conversion.max(YearMonth.parse(studyDocument.getString("ForecastStartMonth")), Conversion.getYearMonth(min.getDate("min")));
				var endMonth = YearMonth.parse(studyDocument.getString("ForecastEndMonth").isEmpty() ? LocalDateTime.now().plusDays(1).format(format) : studyDocument.getString("ForecastEndMonth"), format);

				var months = new ArrayList<YearMonth>();
				for (var m = startMonth; m.compareTo(endMonth) <= 0; m = m.plusMonths(1))
					months.add(m);

				return months.parallelStream().map(m -> {
					var view = String.format("view.verification.%s.%s|%s|%s|%s", environment, study, name, filterName, m.format(format));
					var t = template.replace("{filter}", filter.toJson());
					t = t.replace("{month}", m.format(format));
					t = t.replace("{endMonth}", m.plusMonths(2).format(format));
					t = t.replace("{startTime}", startTime);
					t = t.replace("{endTime}", endTime);
					t = t.replace("{eventTime}", eventTime);
					t = t.replace("{eventValue}", eventValue);
					t = t.replace("{locationMap}", locationMap);
					t = t.replace("{observedClass}", observedClass);
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
				});
			}
			return Stream.empty();
		}).collect(Collectors.toSet());
		existing.parallelStream().filter(e -> !created.contains(e)).forEach(d -> Mongo.dropCollection(database, d));
	}

	@Override
	public String[] getPredecessors() {
		return predecessors;
	}
}
