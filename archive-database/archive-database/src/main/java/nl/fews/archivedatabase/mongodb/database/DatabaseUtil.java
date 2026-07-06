package nl.fews.archivedatabase.mongodb.database;

import com.mongodb.*;

import java.util.*;
import java.util.function.Supplier;
import java.util.regex.Pattern;

/**
 *
 */
public final class DatabaseUtil {

	/**
	 *
	 */
	private static final int NUM_RETRIES = 5;

	/**
	 *
	 */
	private static final int RETRY_INTERVAL_MS = 1000;

	/**
	 *
	 */
	private static final Pattern dupKeyPattern = Pattern.compile("(\\{.*})");

	/**
	 *
	 */
	private DatabaseUtil(){}

	/**
	 *
	 * @param operation operation
	 * @return Document
	 */
	public static <T> T retry(Supplier<T> operation) {
        for (int i = 1; i <= NUM_RETRIES; i++) {
            try {
                return operation.get();
            }
			catch (Exception ex) {
                if (i == NUM_RETRIES ||
					(ex instanceof MongoWriteException && ((MongoWriteException)ex).getError().getCategory() == ErrorCategory.DUPLICATE_KEY) ||
					(ex instanceof MongoBulkWriteException && ((MongoBulkWriteException)ex).getWriteErrors().stream().anyMatch(e -> e.getCategory() == ErrorCategory.DUPLICATE_KEY))){
					if (ex instanceof MongoWriteException){
						var matcher = dupKeyPattern.matcher(((MongoWriteException)ex).getError().getMessage());
						var key = matcher.find() ? matcher.group(1).replace("\",\"", "\\\",\\\"").replace("\"[\"", "\"[\\\"").replace("\"]\"", "\\\"]\"") : "";
						throw new RuntimeException(key, ex);
					}
					else
						throw new RuntimeException(ex);
				}

                try {
                    Thread.sleep(RETRY_INTERVAL_MS);
                }
				catch (InterruptedException iex) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(iex);
                }
            }
        }
        throw new IllegalStateException("Failed Query");
    }
}
