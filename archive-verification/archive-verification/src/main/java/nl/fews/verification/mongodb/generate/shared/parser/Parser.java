package nl.fews.verification.mongodb.generate.shared.parser;

public final class Parser {

	private Parser(){}

	/**
	 * Parses the input object to a float value.
	 *
	 * @param v the object to parse
	 * @return the parsed float value, or the input object if parsing fails
	 */
	public static Object parseFloat(Object v) {
		if (v instanceof String s) {
			try {
				return !s.isEmpty() && Character.isAlphabetic(s.charAt(0)) ? s : Float.parseFloat(s);
			}
			catch (NumberFormatException e) {
				return v;
			}
		}
		return v;
	}
}
