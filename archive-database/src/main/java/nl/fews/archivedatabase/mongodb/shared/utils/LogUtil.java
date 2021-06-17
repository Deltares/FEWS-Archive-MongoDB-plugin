package nl.fews.archivedatabase.mongodb.shared.utils;

import com.mongodb.MongoWriteException;
import org.json.JSONObject;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public final class LogUtil {

	/**
	 *
	 */
	private LogUtil(){}

	/**
	 *
	 * @param ex ex
	 * @param extra extra
	 * @return String
	 */
	public static JSONObject getLogMessageJson(Exception ex, Map<String, Object> extra){
		JSONObject message = new JSONObject(extra);
		message.put("errorMessage", ex.getMessage());
		message.put("stackTrace", Arrays.stream(ex.getStackTrace()).map(StackTraceElement::toString).collect(Collectors.toList()));
		return message;
	}

	/**
	 *
	 * @param ex ex
	 * @param extra extra
	 * @return String
	 */
	public static JSONObject getLogMessageJson(MongoWriteException ex, Map<String, Object> extra){
		JSONObject message = new JSONObject(extra);
		message.put("errorMessage", ex.getError().getMessage());
		message.put("errorDetails", ex.getError().getDetails());
		message.put("errorCategory", ex.getError().getCategory().toString());
		message.put("stackTrace", Arrays.stream(ex.getStackTrace()).map(StackTraceElement::toString).collect(Collectors.toList()));
		return message;
	}
}
