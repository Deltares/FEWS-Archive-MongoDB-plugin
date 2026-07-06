package nl.fews.archivedatabase.common.shared.utils;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

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
	public static Map<String, Object> getLogMessageJson(Exception ex, Map<String, Object> extra){
		var message = new LinkedHashMap<>(extra);
		message.put("errorMessage", ex.getMessage());
		message.put("stackTrace", Arrays.stream(ex.getStackTrace()).map(StackTraceElement::toString).toList());
		return message;
	}
}
