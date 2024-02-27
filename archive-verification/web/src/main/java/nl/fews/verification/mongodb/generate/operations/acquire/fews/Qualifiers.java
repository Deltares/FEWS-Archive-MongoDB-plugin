package nl.fews.verification.mongodb.generate.operations.acquire.fews;

import com.mongodb.client.model.ReplaceOptions;
import nl.fews.verification.mongodb.generate.interfaces.IExecute;
import nl.fews.verification.mongodb.generate.interfaces.IPredecessor;
import nl.fews.verification.mongodb.shared.database.Fews;
import nl.fews.verification.mongodb.shared.database.Mongo;
import org.bson.Document;

@SuppressWarnings("unused")
public final class Qualifiers implements IExecute, IPredecessor {

	private final String[] predecessors = new String[]{};

	/**
	 * Executes the method.
	 *
	 * This method is responsible for replacing a document in the "Verification" collection
	 * with the class simple name as the key. It uses the Fews.getQualifiers() method to retrieve
	 * the qualifiers to be used as filter criteria for replacing the document.
	 * If no document with the specified key is found in the collection, a new document is inserted
	 * using the upsert option. If a document with the specified key already exists, it is replaced with
	 * the new document.
	 *
	 * This method is implemented in the Qualifiers class and overrides the execute() method from the
	 * IExecute interface. The method does not return any value.
	 */
	@Override
	public void execute() {
		Mongo.replaceOne(String.format("fews.%s", getClass().getSimpleName()), new Document(), Fews.getQualifiers(), new ReplaceOptions().upsert(true));
	}

	/**
	 * Returns an array of strings representing the predecessors of the current instance.
	 *
	 * @return an array of strings representing the predecessors of the current instance
	 */
	@Override
	public String[] getPredecessors() {
		return predecessors;
	}
}
