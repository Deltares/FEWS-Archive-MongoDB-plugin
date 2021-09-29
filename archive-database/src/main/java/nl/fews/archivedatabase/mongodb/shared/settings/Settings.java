package nl.fews.archivedatabase.mongodb.shared.settings;

import org.json.JSONObject;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 *
 */
@SuppressWarnings({"unchecked"})
public final class Settings {

	/**
	 * settings collection
	 */
	private static final Map<String, Object> map = new HashMap<>();

	/**
	 * static class
	 */
	private Settings(){}

	/**
	 *
	 * @param key key
	 * @param value object value
	 */
	public static <T> void put(String key, T value) {
		map.put(key, value);
	}

	/**
	 *
	 * @param key key
	 * @param t type based on t.class
	 * @return typed value based on t
	 */
	public static <T> T get(String key, Class<T> t) {
		return (T)map.get(key);
	}

	/**
	 *
	 * @param key key
	 * @return typed value
	 */
	public static <T> T get(String key) {
		return (T)map.get(key);
	}

	/**
	 * @param indentFactor indentFactor
	 * @return String
	 */
	public static String toJsonString(int indentFactor){
		return new JSONObject(map.entrySet().stream().filter(s -> !s.getKey().toLowerCase().contains("password")).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))).toString(indentFactor);
	}
}
