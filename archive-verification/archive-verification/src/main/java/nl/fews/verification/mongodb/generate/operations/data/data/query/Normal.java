package nl.fews.verification.mongodb.generate.operations.data.data.query;

import nl.fews.verification.mongodb.generate.shared.conversion.Conversion;
import nl.fews.verification.mongodb.shared.database.Mongo;
import nl.fews.verification.mongodb.shared.settings.Settings;
import org.bson.Document;

import java.time.YearMonth;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class Normal {

	private Normal(){}

    public static List<Document> getNormalData(Document studyDocument, YearMonth month, String locationId){
		Document filter = Mongo.findOne("Normal", new Document("Name", studyDocument.getString("Normal")));
		var collection = filter.getString("Collection");
		var startTime = Conversion.getStartTime(studyDocument.getString("Time"));
		var endTime = Conversion.getEndTime(studyDocument.getString("Time"));
		var eventTime = Conversion.getEventTime(studyDocument.getString("Time"));
		var eventValue = Conversion.getEventValue(studyDocument.getString("Value"));
		var normalClass = studyDocument.getString("Class").isEmpty() ? null : new Document(Mongo.findOne("Class", new Document("Name", studyDocument.getString("Class"))).getList("Locations", Document.class).stream().collect(Collectors.toMap(c -> c.getString("Location"), c -> c.getList("Breakpoint", Document.class))));
		var startMonth = Conversion.getYearMonthDate(month);
		var endMonth = Conversion.getYearMonthDate(month.plusMonths(1));
		var db = Settings.get("archiveDb", String.class);

		var results = new ArrayList<Document>();
		filter.getList("Filters", Document.class).forEach(f -> {
			var locationMap = f.get("LocationMap", Document.class);
			var documents = new ArrayList<Document>();
			var pipeline = List.of(
				new Document("$match", f.get("Filter", Document.class).append("locationId", locationId).append(endTime, new Document("$gte", startMonth)).append(startTime, new Document("$lt", endMonth))),
				new Document("$project", new Document("_id", 0).append(startTime, 1).append("timeStepMinutes", "$metaData.timeStepMinutes").append(String.format("timeseries.%s", eventTime), 1).append(String.format("timeseries.%s", eventValue), 1)),
				new Document("$sort", new Document(startTime, 1))
			);
			Mongo.aggregate(db, collection, pipeline).forEach(r -> {
				r.append("location", locationMap.get(locationId, locationId)).append("timeseries", fFillNormalTimeseries(r, eventTime, eventValue));
				documents.add(r);
			});
			results.addAll(unwindNormalTimeseries(documents, normalClass));
		});

		return results;
	}

	private static List<Document> fFillNormalTimeseries(Document normal, String eventTime, String eventValue) {
		return Observed.fFillObservedTimeseries(normal, eventTime, eventValue);
	}

	private static List<Document> unwindNormalTimeseries(List<Document> normal, Document normalClass){
		var unwound = new ArrayList<Document>();
		for (var d: normal) {
			for (var t: d.getList("timeseries", Document.class)) {
				unwound.add(
					new Document()
					.append("isOriginalForecast", t.getBoolean("isOriginalObserved"))
					.append("forecastTime", t.getDate("eventTime"))
					.append("forecast", t.getDouble("observed"))
					.append("forecastClass", Common.getValueClass(normalClass, t.getDouble("observed"), d.getString("location"))));
			}
		}
		return unwound;
	}
}
