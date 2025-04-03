package nl.fews.verification.mongodb.generate.operations.data.data.query;

import nl.fews.verification.mongodb.generate.shared.conversion.Conversion;
import nl.fews.verification.mongodb.shared.database.Mongo;
import nl.fews.verification.mongodb.shared.settings.Settings;
import org.bson.Document;

import java.time.Instant;
import java.time.YearMonth;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

public class Observed {

	private Observed(){}

    public static List<Document> getObservedData(Document studyDocument, YearMonth month, String locationId){
		Document filter = Mongo.findOne("Observed", new Document("Name", studyDocument.getString("Observed")));
		var collection = filter.getString("Collection");
		var startTime = Conversion.getStartTime(studyDocument.getString("Time"));
		var endTime = Conversion.getEndTime(studyDocument.getString("Time"));
		var eventTime = Conversion.getEventTime(studyDocument.getString("Time"));
		var eventValue = Conversion.getEventValue(studyDocument.getString("Value"));
		var observedClass = studyDocument.getString("Class").isEmpty() ? null : new Document(Mongo.findOne("Class", new Document("Name", studyDocument.getString("Class"))).getList("Locations", Document.class).stream().collect(Collectors.toMap(c -> c.getString("Location"), c -> c.getList("Breakpoint", Document.class))));
		var startMonth = Conversion.getYearMonthDate(month);
		var endMonth = Conversion.getYearMonthDate(month.plusMonths(2));
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
				r.append("location", locationMap.get(locationId, locationId)).append("timeseries", fFillObservedTimeseries(r, eventTime, eventValue));
				documents.add(r);
			});
			results.addAll(unwindObservedTimeseries(documents, observedClass));
		});
		return results;
	}

	static List<Document> fFillObservedTimeseries(Document observed, String eventTime, String eventValue){
		var timeseries = new ArrayList<Document>();
		var m = observed.getInteger("timeStepMinutes");
		var v = Double.NaN;
		var l = 0;
		var i = 0;
		var t = observed.getList("timeseries", Document.class);
		for (Instant d = t.get(0).getDate(eventTime).toInstant(); d.compareTo(t.get(t.size()-1).getDate(eventTime).toInstant()) <= 0; d = d.plusSeconds(m * 60L)) {
			var et = t.get(i).getDate(eventTime).toInstant();
			var ev = t.get(i).getDouble(eventValue);
			if (d.equals(et)){
				if (ev != null && Double.isFinite(ev)) {
					v = ev;
					l = 0;
				}
				i++;
			}
			l += m;
			if (l <= 24 * 60)
				timeseries.add(new Document("eventTime", Date.from(d)).append("observed", v).append("isOriginalObserved", l == m));
			else
				timeseries.add(new Document("eventTime", Date.from(d)).append("observed", null).append("isOriginalObserved", true));
		}
		return timeseries;
	}

	private static List<Document> unwindObservedTimeseries(List<Document> observed, Document observedClass){
		var unwound = new ArrayList<Document>();
		for (var d: observed) {
			for (var t: d.getList("timeseries", Document.class)) {
				unwound.add(
					new Document()
					.append("isOriginalObserved", t.getBoolean("isOriginalObserved"))
					.append("eventTime", t.getDate("eventTime"))
					.append("observed", t.getDouble("observed"))
					.append("observedClass", Common.getValueClass(observedClass, t.getDouble("observed"), d.getString("location"))));
			}
		}
		return unwound;
	}
}
