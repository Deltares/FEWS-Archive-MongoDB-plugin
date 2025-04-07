package nl.fews.verification.mongodb.generate.operations.data.data.query;

import nl.fews.verification.mongodb.generate.shared.conversion.Conversion;
import nl.fews.verification.mongodb.shared.database.Mongo;
import nl.fews.verification.mongodb.shared.settings.Settings;
import org.bson.Document;

import java.time.Instant;
import java.time.YearMonth;
import java.util.*;
import java.util.stream.Collectors;

import static nl.fews.verification.mongodb.generate.operations.data.data.query.Common.getLocationIdQuery;

public class Observed {

	private Observed(){}

    public static List<Document> getData(Document studyDocument, YearMonth month, String mappedLocationId){
		Document observed = Mongo.findOne("Observed", new Document("Name", studyDocument.getString("Observed")));
		var collection = observed.getString("Collection");
		var startTimeKey = Conversion.getStartTime(studyDocument.getString("Time"));
		var endTimeKey = Conversion.getEndTime(studyDocument.getString("Time"));
		var eventTimeKey = Conversion.getEventTime(studyDocument.getString("Time"));
		var eventValueKey = Conversion.getEventValue(studyDocument.getString("Value"));
		var _class = studyDocument.getString("Class").isEmpty() ? null : new Document(Mongo.findOne("Class", new Document("Name", studyDocument.getString("Class"))).getList("Locations", Document.class).stream().collect(Collectors.toMap(c -> c.getString("Location"), c -> c.getList("Breakpoint", Document.class))));
		var startMonth = Conversion.getYearMonthDate(month);
		var endMonth = Conversion.getYearMonthDate(month.plusMonths(2));
		var db = Settings.get("archiveDb", String.class);

		var results = new ArrayList<Document>();
		for (var filter: observed.getList("Filters", Document.class)) {
			var match = filter.get("Filter", Document.class);
			var locationMap = filter.get("LocationMap", Document.class);
			var locationIdQuery = getLocationIdQuery(filter, locationMap, mappedLocationId);
			if (locationIdQuery == null)
				continue;
			var pipeline = List.of(
				new Document("$match", match.append("locationId", locationIdQuery).append(endTimeKey, new Document("$gte", startMonth)).append(startTimeKey, new Document("$lt", endMonth))),
				new Document("$project", new Document("_id", 0).append("locationId", 1).append(startTimeKey, 1).append("timeStepMinutes", "$metaData.timeStepMinutes").append(String.format("timeseries.%s", eventTimeKey), 1).append(String.format("timeseries.%s", eventValueKey), 1)),
				new Document("$sort", new Document(startTimeKey, 1))
			);
			var documents = new ArrayList<Document>();
			Mongo.aggregate(db, collection, pipeline).forEach(r -> {
				r.append("location", locationMap.get(r.getString("locationId"), r.getString("locationId"))).append("timeseries", fFillTimeseries(r, eventTimeKey, eventValueKey));
				documents.add(r);
			});
			results.addAll(unwindTimeseries(documents, _class));
		}
		return results;
	}

	static List<Document> fFillTimeseries(Document observed, String eventTime, String eventValue){
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
				timeseries.add(new Document("et", Date.from(d)).append("ov", v).append("oo", l == m));
			else
				timeseries.add(new Document("et", Date.from(d)).append("ov", null).append("oo", true));
		}
		return timeseries;
	}

	private static List<Document> unwindTimeseries(List<Document> observeds, Document observedClass){
		var unwound = new ArrayList<Document>();
		for (var observed: observeds) {
			for (var timeseries: observed.getList("timeseries", Document.class)) {
				unwound.add(
					new Document()
					.append("oo", timeseries.getBoolean("oo"))
					.append("et", timeseries.getDate("et"))
					.append("ov", timeseries.getDouble("ov"))
					.append("oc", Common.getValueClass(observedClass, timeseries.getDouble("ov"), observed.getString("location"))));
			}
		}
		return unwound;
	}
}
