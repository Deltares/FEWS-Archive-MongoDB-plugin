package nl.fews.verification.mongodb.generate.operations.acquire.fews;

import com.mongodb.client.model.ReplaceOptions;
import nl.fews.verification.mongodb.generate.interfaces.IExecute;
import nl.fews.verification.mongodb.generate.interfaces.IPredecessor;
import nl.fews.verification.mongodb.shared.database.Fews;
import nl.fews.verification.mongodb.shared.database.Mongo;
import org.bson.Document;

@SuppressWarnings("unused")
public final class Locations implements IExecute, IPredecessor {

	private final String[] predecessors = new String[]{};

	/**
	 * Executes the `execute` method.
	 * This method replaces a document in the "Verification" collection
	 * in a Mongo database with a new document based on the specified criteria and options.
	 * The document is upserted, meaning that if no matching document is found,
	 * a new document is inserted with the specified criteria and options.
	 * The collection name is determined by the simple name of the class implementing this method.
	 * The replacement document is empty, and the criteria for matching documents is also empty.
	 * The replacement options are obtained from the `Fews.getLocations()` method.
	 */
	@Override
	public void execute() {
		Mongo.replaceOne(String.format("fews.%s", getClass().getSimpleName()), new Document(), Fews.getLocations(), new ReplaceOptions().upsert(true));
	}

	/**
	 * Retrieves the array of predecessors associated with this object.
	 *
	 * @return an array of strings representing the predecessors of this object.
	 */
	@Override
	public String[] getPredecessors() {
		return predecessors;
	}
}
