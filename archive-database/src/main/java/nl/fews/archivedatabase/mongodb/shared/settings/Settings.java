package nl.fews.archivedatabase.mongodb.shared.settings;

import java.util.HashMap;
import java.util.Map;

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
}
