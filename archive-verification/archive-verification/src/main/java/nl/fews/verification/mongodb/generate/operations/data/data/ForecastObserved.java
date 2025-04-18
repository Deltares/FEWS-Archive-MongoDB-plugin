package nl.fews.verification.mongodb.generate.operations.data.data;

import nl.fews.verification.mongodb.generate.interfaces.IExecute;
import nl.fews.verification.mongodb.generate.interfaces.IPredecessor;
import nl.fews.verification.mongodb.generate.operations.data.data.query.Forecast;
import nl.fews.verification.mongodb.generate.operations.data.data.query.Observed;
import nl.fews.verification.mongodb.generate.operations.data.data.query.Normal;
import nl.fews.verification.mongodb.generate.shared.conversion.Conversion;
import nl.fews.verification.mongodb.shared.database.Mongo;
import nl.fews.verification.mongodb.shared.database.MongoIndex;
import nl.fews.verification.mongodb.shared.settings.Settings;
import org.bson.Document;

import java.time.LocalDateTime;
import java.time.YearMonth;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public final class ForecastObserved implements IExecute, IPredecessor {

	private final String[] predecessors = new String[]{};
	private final String study;

	public ForecastObserved(String study){
		this.study = study;
	}

	@Override
	public void execute(){
		var name = this.getClass().getSimpleName();
		var collection = String.format("verification.%s_%s", study, name);
		var studyDocument = Mongo.findOne("Study", new Document("Name", study));
		var format = Conversion.getMonthDateTimeFormatter();
		var months = getMonthsToProcess(studyDocument, study, collection);
		boolean parallel = Settings.get("parallel", Boolean.class);
		ensureCollection(collection);

		if (parallel) {
			var pool = Executors.newFixedThreadPool(Settings.get("dataParallelPartitions", Integer.class));
			try {
				List<Future<Object>> results = pool.invokeAll(months.stream().map(m -> (Callable<Object>) () -> query(m, format, collection, studyDocument)).toList());
				for (Future<Object> x : results) {
					x.get();
				}
			}
			catch (InterruptedException | ExecutionException ex) {
				Thread.currentThread().interrupt();
				throw new RuntimeException(ex);
			}
			finally {
				pool.shutdown();
			}
		}
		else {
			for (var m: months){
				query(m, format, collection, studyDocument);
			}
		}
	}

	private Object query(YearMonth m, DateTimeFormatter format, String collection, Document studyDocument){
		var lastModified = new Date();
		var month = m.format(format);
		var tempMonth = String.format("%s_temp", month);

		Mongo.deleteMany(collection, new Document("month", tempMonth));

		for (var mappedLocationId: getMappedLocationIds(studyDocument, m)) {
			var forecasts = Forecast.getData(studyDocument, m, mappedLocationId);
			var observedLookup = Observed.getData(studyDocument, m, mappedLocationId).stream().collect(Collectors.toMap(t -> t.getDate("et"), t -> t, (a, b) -> a));
			insertForecastObserved(forecasts, observedLookup, tempMonth, lastModified, collection);

			var normalForecasts = collapseForecastTime(forecasts);
			forecasts.clear();
			var normalLookup = Normal.getData(studyDocument, m, mappedLocationId).stream().collect(Collectors.toMap(t -> t.getDate("ft"), t -> t, (a, b) -> a));
			insertNormalForecastObserved(normalForecasts, observedLookup, normalLookup, tempMonth, lastModified, collection);
		}
		Mongo.deleteMany(collection, new Document("month", month));
		Mongo.updateMany(collection, new Document("month", tempMonth), new Document("$set", new Document("month", month)));
		return null;
	}

	private static void insertForecastObserved(List<Document> forecasts, Map<Date, Document> observedLookup, String tempMonth, Date lastModified, String collection){
		var forecastObserved = new ArrayList<Document>();
		for (var forecast : forecasts) {
			forecast.append("month", tempMonth).append("lastModified", lastModified);
			var ts = new ArrayList<Document>();
			for (var timeseries: forecast.getList("timeseries", Document.class)) {
				if (observedLookup.containsKey(timeseries.getDate("et"))) {
					var o = observedLookup.get(timeseries.getDate("et"));
					timeseries
						.append("oo", o.getBoolean("oo"))
						.append("ov", o.getDouble("ov"))
						.append("oc", o.getString("oc"));
					ts.add(timeseries);
				}
			}
			if (!ts.isEmpty()) {
				forecast.append("timeseries", ts);
				forecastObserved.add(forecast);
			}
		}
		if (!forecastObserved.isEmpty())
			Mongo.insertMany(collection, forecastObserved);
	}

	private static void insertNormalForecastObserved(List<Document> normalForecasts, Map<Date, Document> observedLookup, Map<Date, Document> normalLookup, String tempMonth, Date lastModified, String collection){
		var forecastObserved = new ArrayList<Document>();
		for (var forecast: normalForecasts){
			forecast.remove("_id");
			forecast.append("month", tempMonth).append("lastModified", lastModified).append("forecastName", "Normal").append("forecastId", "Normal").append("ensemble", "").append("ensembleMember", "");
			var ts = new ArrayList<Document>();
			for (var timeseries: forecast.getList("timeseries", Document.class)) {
				if (normalLookup.containsKey(forecast.getDate("forecastTime")) && observedLookup.containsKey(timeseries.getDate("et"))) {
					var n = normalLookup.get(forecast.getDate("forecastTime"));
					var o = observedLookup.get(timeseries.getDate("et"));
					timeseries
						.append("of", n.getBoolean("of"))
						.append("fv", n.getDouble("fv"))
						.append("fc", n.getString("fc"))
						.append("oo", o.getBoolean("oo"))
						.append("ov", o.getDouble("ov"))
						.append("oc", o.getString("oc"));
					ts.add(timeseries);
				}
			}
			if (!ts.isEmpty()) {
				forecast.append("timeseries", ts);
				forecastObserved.add(forecast);
			}
		}
		if (!forecastObserved.isEmpty())
			Mongo.insertMany(collection, forecastObserved);
	}

	private static List<String> getMappedLocationIds(Document studyDocument, YearMonth month){
		var forecastTime = Conversion.getForecastTime(studyDocument.getString("Time"));
		var startMonth = Conversion.getYearMonthDate(month);
		var endMonth = Conversion.getYearMonthDate(month.plusMonths(1));

		var locations = StreamSupport.stream(Mongo.find("Forecast", new Document("Name", new Document("$in", studyDocument.getList("Forecasts", String.class)))).spliterator(), false).flatMap(forecast ->
			forecast.getList("Filters", Document.class).stream().flatMap(filter ->
				StreamSupport.stream(Mongo.distinct(Settings.get("archiveDb", String.class), forecast.getString("Collection"), "locationId",
					filter.get("Filter", Document.class).append(forecastTime, new Document("$gte", startMonth).append("$lt", endMonth)), String.class).spliterator(), false).map(location ->
						filter.get("LocationMap", Document.class).get(location, location)))).toList();
		return new HashSet<>(locations).stream().sorted().toList();
	}

	private static List<Document> collapseForecastTime(List<Document> forecasts){
		return forecasts.stream().collect(Collectors.groupingBy((Document f) -> f.getDate("forecastTime"))).values().stream().map(f ->
			f.get(0).append("timeseries", f.stream().flatMap((Document s) ->
				s.getList("timeseries", Document.class).stream()).collect(Collectors.toMap((Document t) ->
					t.getDate("et"), t -> t, (a, b) -> a)).values().stream().sorted(Comparator.comparing(s -> s.getDate("et"))).toList())).sorted(Comparator.comparing(s -> s.getDate("forecastTime"))).toList();
	}

	private static void ensureCollection(String collection){
		MongoIndex.ensureCollection(collection, List.of(
			new Document(Stream.of("forecastId", "forecastTime", "location", "month").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))).append("unique", 1),
			new Document(Stream.of("month").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
			new Document(Stream.of("location").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
			new Document(Stream.of("location", "locationId").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
			new Document(Stream.of("forecastId").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new))),
			new Document(Stream.of("forecastTime").collect(Collectors.toMap(s -> s, s -> 1, (k, v) -> v, LinkedHashMap::new)))
		));
	}

	private static List<YearMonth> getMonthsToProcess(Document studyDocument, String study, String collection){
		var format = Conversion.getMonthDateTimeFormatter();
		var forecastStartMonth = studyDocument.getString("ForecastStartMonth");
		var reprocessCube = Arrays.stream(Settings.get("reprocessCubes", String.class).split(",")).map(String::trim).toList().contains(String.format("Verification_%s", study));
		var startMonth = YearMonth.parse(forecastStartMonth);
		var endMonth = YearMonth.parse(studyDocument.getString("ForecastEndMonth").isEmpty() ? LocalDateTime.now().plusDays(1).format(format) : studyDocument.getString("ForecastEndMonth"), format);
		var reprocessStartMonth = reprocessCube ? startMonth : YearMonth.parse(LocalDateTime.now().minusDays(studyDocument.getInteger("ReprocessDays")).format(format), format);
		var months = new ArrayList<YearMonth>();
		for(var m = startMonth; m.compareTo(endMonth) <= 0; m = m.plusMonths(1)) {
			var month = Mongo.findOne(collection, new Document("month", m.format(format)));
			var lastModified = month != null ? month.getDate("lastModified").toInstant() : null;
			var dataStaleAfterSeconds = Settings.get("dataStaleAfterSeconds", Integer.class);
			if (month == null || lastModified.compareTo(new Date().toInstant().minusSeconds(dataStaleAfterSeconds)) < 0 && m.compareTo(reprocessStartMonth) >= 0) {
				months.add(m);
			}
		}
		return months;
	}

	@Override
	public String[] getPredecessors() {
		return predecessors;
	}
}
