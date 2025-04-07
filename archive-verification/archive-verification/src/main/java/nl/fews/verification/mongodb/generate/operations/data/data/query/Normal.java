package nl.fews.verification.mongodb.generate.operations.data.data.query;

import nl.fews.verification.mongodb.generate.shared.conversion.Conversion;
import nl.fews.verification.mongodb.shared.database.Mongo;
import nl.fews.verification.mongodb.shared.settings.Settings;
import org.bson.Document;


import java.time.YearMonth;
import java.util.*;
import java.util.stream.Collectors;

import static nl.fews.verification.mongodb.generate.operations.data.data.query.Common.getLocationIdQuery;

public class Normal {

	private Normal(){}

    public static List<Document> getData(Document studyDocument, YearMonth month, String mappedLocationId){
		Document normal = Mongo.findOne("Normal", new Document("Name", studyDocument.getString("Normal")));
		var collection = normal.getString("Collection");
		var startTimeKey = Conversion.getStartTime(studyDocument.getString("Time"));
		var endTimeKey = Conversion.getEndTime(studyDocument.getString("Time"));
		var eventTimeKey = Conversion.getEventTime(studyDocument.getString("Time"));
		var eventValueKey = Conversion.getEventValue(studyDocument.getString("Value"));
		var _class = studyDocument.getString("Class").isEmpty() ? null : new Document(Mongo.findOne("Class", new Document("Name", studyDocument.getString("Class"))).getList("Locations", Document.class).stream().collect(Collectors.toMap(c -> c.getString("Location"), c -> c.getList("Breakpoint", Document.class))));
		var startMonth = Conversion.getYearMonthDate(month);
		var endMonth = Conversion.getYearMonthDate(month.plusMonths(1));
		var db = Settings.get("archiveDb", String.class);

		var results = new ArrayList<Document>();
		for (var filter: normal.getList("Filters", Document.class)) {
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

	private static List<Document> fFillTimeseries(Document normal, String eventTime, String eventValue) {
		return Observed.fFillTimeseries(normal, eventTime, eventValue);
	}

	private static List<Document> unwindTimeseries(List<Document> normals, Document normalClass){
		var unwound = new ArrayList<Document>();
		for (var normal: normals) {
			for (var timeseries: normal.getList("timeseries", Document.class)) {
				unwound.add(
					new Document()
					.append("oo", timeseries.getBoolean("oo"))
					.append("ft", timeseries.getDate("et"))
					.append("fv", timeseries.getDouble("ov"))
					.append("fc", Common.getValueClass(normalClass, timeseries.getDouble("observed"), normal.getString("location"))));
			}
		}
		return unwound;
	}
}
