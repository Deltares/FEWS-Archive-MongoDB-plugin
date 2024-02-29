package nl.fews.verification.mongodb.generate.operations.acquire.fews;

import com.mongodb.client.model.ReplaceOptions;
import nl.fews.verification.mongodb.generate.interfaces.IExecute;
import nl.fews.verification.mongodb.generate.interfaces.IPredecessor;
import nl.fews.verification.mongodb.shared.database.Fews;
import nl.fews.verification.mongodb.shared.database.Mongo;
import org.bson.Document;

@SuppressWarnings("unused")
public final class Parameters implements IExecute, IPredecessor {

	private final String[] predecessors = new String[]{};

	/**
	 * Executes the given method.
	 * This method replaces a document in the "Verification" collection of the Mongo database.
	 * The document is replaced based on the simple name of the current class.
	 * If the document doesn't exist, it is created as an upsert operation.
	 */
	@Override
	public void execute() {
		Mongo.replaceOne(String.format("fews.%s", getClass().getSimpleName()), new Document(), Fews.getParameters(), new ReplaceOptions().upsert(true));
	}

	/**
	 * Gets the array of predecessors for the current object.
	 *
	 * @return an array of strings representing the predecessors
	 */
	@Override
	public String[] getPredecessors() {
		return predecessors;
	}
}
