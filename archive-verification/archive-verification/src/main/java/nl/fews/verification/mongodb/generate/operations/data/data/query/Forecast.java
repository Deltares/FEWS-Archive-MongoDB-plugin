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

import static nl.fews.verification.mongodb.generate.operations.data.data.query.Common.getValueClass;

public final class Forecast {

	private Forecast(){}

    public static List<Document> getForecastData(Document studyDocument, YearMonth month, String locationId){
		var forecasts = StreamSupport.stream(Mongo.find("Forecast", new Document("Name", new Document("$in", studyDocument.getList("Forecasts", String.class)))).spliterator(), false).toList();
		var forecastTimeKey = Conversion.getForecastTime(studyDocument.getString("Time"));
		var eventTime = Conversion.getEventTime(studyDocument.getString("Time"));
		var eventValue = Conversion.getEventValue(studyDocument.getString("Value"));
		var forecastClass = studyDocument.getString("Class").isEmpty() ? null : new Document(Mongo.findOne("Class", new Document("Name", studyDocument.getString("Class"))).getList("Locations", Document.class).stream().collect(Collectors.toMap(c -> c.getString("Location"), c -> c.getList("Breakpoint", Document.class))));
		var startMonth = Conversion.getYearMonthDate(month);
		var endMonth = Conversion.getYearMonthDate(month.plusMonths(1));
		var db = Settings.get("archiveDb", String.class);

		var results = new ArrayList<Document>();
		forecasts.forEach(filter -> {
			var forecastName = filter.getString("ForecastName");
			var collection = filter.getString("Collection");
			filter.getList("Filters", Document.class).forEach(f -> {
				var locationMap = f.get("LocationMap", Document.class);
				var pipeline = List.of(
					new Document("$match", f.get("Filter", Document.class).append("locationId", locationId).append(forecastTimeKey, new Document("$gte", startMonth).append("$lt", endMonth))),
					new Document("$project",
						new Document("_id", 0)
							.append("ensemble", "$ensembleId")
							.append("ensembleMember", "$ensembleMemberId")
							.append("forecastTime", String.format("$%s", forecastTimeKey))
							.append("dispatchTime", "$runInfo.dispatchTime")
							.append("timeStepMinutes", "$metaData.timeStepMinutes")
							.append(String.format("timeseries.%s", eventTime), 1)
							.append(String.format("timeseries.%s", eventValue), 1)),
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
						.append("forecastDate", Date.from(forecastDate.toInstant(ZoneOffset.UTC)))
						.append("forecastMinute", forecastMinute)
						.append("forecastId", forecastId)
						.append("forecastName", forecastName)
						.append("location", locationMap.get(locationId, locationId))
						.append("timeseries", fFillForecastTimeseries(r, eventTime, eventValue));

					documents.add(r);
				}
				documents = documents.stream().collect(Collectors.toMap(d -> String.format("%s_%s", d.getString("forecastId"), d.getDate("forecastTime")), d -> d, (a, b) -> b)).entrySet().stream().sorted(Map.Entry.comparingByKey()).map(Map.Entry::getValue).toList();
				results.addAll(getForecastTimeseries(fFillForecast(documents), forecastClass));
			});
		});
		for (var r: results){
			r.remove("dispatchTime");
		}
		return results;
	}
	
	private static List<Document> fFillForecastTimeseries(Document forecast, String eventTime, String eventValue){
		var timeseries = new ArrayList<Document>();
		var ft = forecast.getDate("forecastTime").toInstant();
		var m = forecast.getInteger("timeStepMinutes");
		var v = Double.NaN;
		var l = 0;
		var i = 0;
		var t = forecast.getList("timeseries", Document.class);
		for (Instant d = t.get(0).getDate(eventTime).toInstant(); d.compareTo(t.get(t.size()-1).getDate(eventTime).toInstant()) <= 0; d = d.plusSeconds(m * 60L)) {
			var et = t.get(i).getDate(eventTime).toInstant();
			var ev = t.get(i).getDouble(eventValue);
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
					timeseries.add(new Document("eventTime", Date.from(d)).append("forecast", v).append("isOriginalForecast", l == m));
				else
					timeseries.add(new Document("eventTime", Date.from(d)).append("forecast", null).append("isOriginalForecast", true));
			}
		}
		return timeseries;
	}

	private static List<Document> getForecastTimeseries(List<Document> forecasts, Document forecastClass){
		for (var forecast: forecasts) {
			for (var timeseries: forecast.getList("timeseries", Document.class)) {
				var i = timeseries.getDate("eventTime").toInstant();
				timeseries
					.append("eventDate", Date.from(i.atZone(ZoneOffset.UTC).toLocalDate().atStartOfDay(ZoneOffset.UTC).toInstant()))
					.append("eventMinute", i.atZone(ZoneOffset.UTC).getHour() * 60 + i.atZone(ZoneOffset.UTC).getMinute())
					.append("leadTime", (timeseries.getDate("eventTime").getTime() - forecast.getDate("forecastTime").getTime()) / (60 * 1000))
					.append("forecastClass", getValueClass(forecastClass, timeseries.getDouble("forecast"), forecast.getString("location")));
			}
		}
		return forecasts;
	}
	
	private static List<Document> fFillForecast(List<Document> forecasts) {
		var filled = new ArrayList<Document>();
		Document curr = null;
		for (var partition: forecasts.stream().collect(Collectors.groupingBy(d -> d.getString("forecastId"))).entrySet()) {
			for (var next : partition.getValue().stream().sorted(Comparator.comparing(d -> d.getDate("forecastTime").getTime())).toList()) {
				if (curr != null) {
					filled.add(curr);
					var m = curr.getInteger("timeStepMinutes");
					for (
							Instant i = curr.getDate("forecastTime").toInstant().plusSeconds(m * 60L);
							i.compareTo(next.getDate("forecastTime").toInstant()) < 0 && i.compareTo(curr.getDate("forecastTime").toInstant().plus(24, ChronoUnit.HOURS)) <= 0;
							i = i.plusSeconds(m * 60L)) {
						var copy = Document.parse(curr.toJson());
						copy.append("forecastTime", Date.from(i));
						copy.append("forecastDate", Date.from(i.atZone(ZoneOffset.UTC).toLocalDate().atStartOfDay(ZoneOffset.UTC).toInstant()));
						copy.append("forecastMinute", i.atZone(ZoneOffset.UTC).getHour() * 60 + i.atZone(ZoneOffset.UTC).getMinute());
						Instant fi = i;
						copy.append("timeseries", copy.getList("timeseries", Document.class).stream().filter(t -> t.getDate("eventTime").toInstant().compareTo(fi) >= 0).map(t -> t.append("isOriginalForecast", false)).toList());
						filled.add(copy);
					}
				}
				curr = next;
			}
			filled.add(curr);
		}
		return filled;
	}
}
