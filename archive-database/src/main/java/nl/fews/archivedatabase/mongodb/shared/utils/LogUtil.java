package nl.fews.archivedatabase.mongodb.shared.utils;

import com.mongodb.MongoWriteException;
import org.bson.Document;

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
	public static Document getLogMessageJson(Exception ex, Map<String, Object> extra){
		Document message = new Document(extra);
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
	public static Document getLogMessageJson(MongoWriteException ex, Map<String, Object> extra){
		Document message = new Document(extra);
		message.put("errorMessage", ex.getError().getMessage());
		message.put("errorDetails", ex.getError().getDetails());
		message.put("errorCategory", ex.getError().getCategory().toString());
		message.put("stackTrace", Arrays.stream(ex.getStackTrace()).map(StackTraceElement::toString).collect(Collectors.toList()));
		return message;
	}
}
