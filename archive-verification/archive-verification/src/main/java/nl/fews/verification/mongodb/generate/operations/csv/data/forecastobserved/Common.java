package nl.fews.verification.mongodb.generate.operations.csv.data.forecastobserved;

import org.bson.Document;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;

public final class Common {

	private Common(){}

    public static  List<Document> deduplicateTime(List<Document> documents, String timeField){
		var deduplicated = new ArrayList<Document>();
		var seen = new HashSet<Instant>();
		for (var r : documents) {
			var k = r.getDate(timeField).toInstant();
			if (!seen.contains(k)) {
				seen.add(k);
				deduplicated.add(r);
			}
		}
		return deduplicated;
	}

	public static String getValueClass(Document _class, Double value, String location){
		if (_class == null){
			return "NA";
		}
		var breakpoints = _class.containsKey(location) ? _class.getList(location, Document.class) : _class.getList("", Document.class);
		for (var b : breakpoints) {
			if(evaluateBreakpoint(b, value)){
				return _class.getString("Name");
			}
		}
		return "UNDEFINED";
	}

	private static boolean evaluateBreakpoint(Document breakpoint, Double value){
		Object t = breakpoint.get("Threshold");
		Double threshold = t == null ? null : ((Number)t).doubleValue();
        return switch (breakpoint.getString("Operator")) {
            case "eq" -> Objects.equals(value, threshold);
            case "gte" -> Double.compare(value, threshold) >= 0;
            case "gt" -> Double.compare(value, threshold) > 0;
            case "lt" -> Double.compare(value, threshold) < 0;
            case "lte" -> Double.compare(value, threshold) <= 0;
            default -> throw new IllegalArgumentException(breakpoint.getString("Operator"));
        };
	}
}
