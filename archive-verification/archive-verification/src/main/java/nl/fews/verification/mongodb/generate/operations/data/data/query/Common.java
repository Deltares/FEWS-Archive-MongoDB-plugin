package nl.fews.verification.mongodb.generate.operations.data.data.query;

import org.bson.Document;

import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public final class Common {

	private Common(){}

	public static Object getLocationIdQuery(Document match, Document locationMap, String mappedLocationId) {
		var queryLocationIds = locationMap.entrySet().stream().filter(f -> f.getValue().equals(mappedLocationId)).map(Map.Entry::getKey).collect(Collectors.toList());
			if (queryLocationIds.isEmpty())
				queryLocationIds.add(mappedLocationId) ;

		if (match.containsKey("locationId")) {
			var applicableLocationIds = match.get("locationId") instanceof String ? Set.of(match.getString("locationId")) : new HashSet<>(match.get("locationId", Document.class).getList("$in", String.class));
			queryLocationIds = queryLocationIds.stream().filter(applicableLocationIds::contains).toList();
		}
		if (queryLocationIds.isEmpty())
			return null;
		return queryLocationIds.size() == 1 ? queryLocationIds.get(0) : new Document("$in", queryLocationIds);
	}

	public static String getValueClass(Document _class, Double value, String location){
		if (_class == null){
			return "NA";
		}
		var breakpoints = _class.containsKey(location) ? _class.getList(location, Document.class) : _class.getList("", Document.class);
		for (var b : breakpoints) {
			if(evaluateBreakpoint(b, value)){
				return b.getString("Name");
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
