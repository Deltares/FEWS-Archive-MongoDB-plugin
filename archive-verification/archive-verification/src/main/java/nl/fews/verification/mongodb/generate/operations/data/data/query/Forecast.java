package nl.fews.verification.mongodb.generate.operations.data.data.query;

import nl.fews.verification.mongodb.generate.shared.conversion.Conversion;
import nl.fews.verification.mongodb.shared.database.Mongo;
import nl.fews.verification.mongodb.shared.settings.Settings;
import org.bson.Document;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.YearMonth;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static nl.fews.verification.mongodb.generate.operations.data.data.query.Common.getLocationIdQuery;
import static nl.fews.verification.mongodb.generate.operations.data.data.query.Common.getValueClass;

public final class Forecast {

	private Forecast(){}

    public static List<Document> getData(Document studyDocument, YearMonth month, String mappedLocationId){
		var forecasts = StreamSupport.stream(Mongo.find("Forecast", new Document("Name", new Document("$in", studyDocument.getList("Forecasts", String.class)))).spliterator(), false).toList();
		var forecastTimeKey = Conversion.getForecastTime(studyDocument.getString("Time"));
		var eventTimeKey = Conversion.getEventTime(studyDocument.getString("Time"));
		var eventValueKey = Conversion.getEventValue(studyDocument.getString("Value"));
		var _class = studyDocument.getString("Class").isEmpty() ? null : new Document(Mongo.findOne("Class", new Document("Name", studyDocument.getString("Class"))).getList("Locations", Document.class).stream().collect(Collectors.toMap(c -> c.getString("Location"), c -> c.getList("Breakpoint", Document.class))));
		var maxLeadTimeMinutes = studyDocument.getInteger("MaxLeadTimeMinutes");
		var startMonth = Conversion.getYearMonthDate(month);
		var endMonth = Conversion.getYearMonthDate(month.plusMonths(1));
		var db = Settings.get("archiveDb", String.class);

		var results = new ArrayList<Document>();
		for (var forecast : forecasts) {
			var forecastName = forecast.getString("ForecastName");
			var collection = forecast.getString("Collection");
			for (var filter: forecast.getList("Filters", Document.class)) {
				var match  = filter.get("Filter", Document.class);
				var locationMap = filter.get("LocationMap", Document.class);
				var locationIdQuery = getLocationIdQuery(match, locationMap, mappedLocationId);
				if (locationIdQuery == null)
					continue;
				var pipeline = List.of(
					new Document("$match", match.append("locationId", locationIdQuery).append(forecastTimeKey, new Document("$gte", startMonth).append("$lt", endMonth))),
					new Document("$project", new Document("_id", 0).append("locationId", "$locationId").append("ensemble", "$ensembleId").append("ensembleMember", "$ensembleMemberId").append("forecastTime", String.format("$%s", forecastTimeKey)).append("dispatchTime", "$runInfo.dispatchTime").append("timeStepMinutes", "$metaData.timeStepMinutes").append(String.format("timeseries.%s", eventTimeKey), 1).append(String.format("timeseries.%s", eventValueKey), 1)),
					new Document("$sort", new Document("forecastTime", 1).append("dispatchTime", 1))
				);
				List<Document> documents = new ArrayList<>();
				for (var r: Mongo.aggregate(db, collection, pipeline)){
					var forecastId = String.join("_", Stream.of(forecastName, r.getString("ensembleId"), r.getString("ensembleMemberId")).filter(s -> s != null && !s.isEmpty()).toList());
					var forecastTime = LocalDateTime.ofInstant(r.getDate("forecastTime").toInstant(), ZoneOffset.UTC);
					var forecastDate = forecastTime.truncatedTo(ChronoUnit.DAYS);
					var timeStepMinutes = r.getInteger("timeStepMinutes");
					var forecastMinute = (forecastTime.getHour() * 60 + forecastTime.getMinute()) / timeStepMinutes * timeStepMinutes;
					forecastTime = forecastDate.plusMinutes(forecastMinute);

					r.append("forecastTime", Date.from(forecastTime.toInstant(ZoneOffset.UTC)))
						.append("forecastId", forecastId)
						.append("forecastName", forecastName)
						.append("location", locationMap.get(r.getString("locationId"), r.getString("locationId")))
						.append("timeseries", fFillTimeseries(r, eventTimeKey, eventValueKey, maxLeadTimeMinutes));

					documents.add(r);
				}
				documents = documents.stream().collect(Collectors.toMap(d -> String.format("%s_%s", d.getString("forecastId"), d.getDate("forecastTime")), d -> d, (a, b) -> b)).entrySet().stream().sorted(Map.Entry.comparingByKey()).map(Map.Entry::getValue).toList();
				results.addAll(getClass(fFillForecast(documents, month), _class));
			}
		}
		for (var r: results){
			r.remove("dispatchTime");
		}
		return results;
	}
	
	private static List<Document> fFillTimeseries(Document forecast, String eventTimeKey, String eventValueKey, Integer maxLeadTimeMinutes) {
		var timeseries = new ArrayList<Document>();
		var ft = forecast.getDate("forecastTime").toInstant();
		var m = forecast.getInteger("timeStepMinutes");
		var v = Double.NaN;
		var l = 0;
		var i = 0;
		var t = forecast.getList("timeseries", Document.class);

		var endDate = t.get(t.size()-1).getDate(eventTimeKey).toInstant();
		var maxDate = ft.plusSeconds(maxLeadTimeMinutes * 60L);
		endDate = endDate.isBefore(maxDate) ? endDate : maxDate;

		for (Instant d = t.get(0).getDate(eventTimeKey).toInstant(); d.compareTo(endDate) <= 0; d = d.plusSeconds(m * 60L)) {
			var et = t.get(i).getDate(eventTimeKey).toInstant();
			var ev = t.get(i).getDouble(eventValueKey);
			if (d.equals(et)) {
				if (ev != null && Double.isFinite(ev)) {
					v = ev;
					l = 0;
				}
				i++;
			}
			l += m;
			if (d.compareTo(ft) >= 0) {
				if (l <= 24 * 60)
					timeseries.add(new Document("et", Date.from(d)).append("fv", v).append("of", l == m));
				else
					timeseries.add(new Document("et", Date.from(d)).append("fv", null).append("of", true));
			}
		}
		return timeseries;
	}

	private static List<Document> getClass(List<Document> forecasts, Document _class){
		for (var forecast: forecasts) {
			for (var timeseries: forecast.getList("timeseries", Document.class)) {
				timeseries.append("fc", getValueClass(_class, timeseries.getDouble("fv"), forecast.getString("location")));
			}
		}
		return forecasts;
	}
	
	private static List<Document> fFillForecast(List<Document> forecasts, YearMonth month) {
		var filled = new ArrayList<Document>();
		var nextMonth = Conversion.getYearMonthDate(month.plusMonths(1));
		Document curr = null;
		for (var partition: forecasts.stream().collect(Collectors.groupingBy(d -> d.getString("forecastId"))).entrySet()) {
			for (var next : partition.getValue().stream().sorted(Comparator.comparing(d -> d.getDate("forecastTime").getTime())).toList()) {
				if (curr == null) {
					curr = next;
					continue;
				}
				filled.add(curr);
				var m = curr.getInteger("timeStepMinutes");
				for (
					Instant i = curr.getDate("forecastTime").toInstant().plusSeconds(m * 60L);
					i.compareTo(next.getDate("forecastTime").toInstant()) < 0 && i.compareTo(curr.getDate("forecastTime").toInstant().plus(24, ChronoUnit.HOURS)) <= 0;
					i = i.plusSeconds(m * 60L)
				) {
					Instant fi = i;
					if (Date.from(fi).compareTo(nextMonth) >= 0)
						continue;
					var copy = Document.parse(curr.toJson());
					copy.append("forecastTime", Date.from(fi));
					copy.append("timeseries", copy.getList("timeseries", Document.class).stream().filter(t -> t.getDate("et").toInstant().compareTo(fi) >= 0).map(t -> t.append("of", false)).toList());
					filled.add(copy);
				}
				curr = next;
			}
			filled.add(curr);
		}
		return filled;
	}
}
