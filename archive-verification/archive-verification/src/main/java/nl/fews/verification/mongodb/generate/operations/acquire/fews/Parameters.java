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

	@Override
	public void execute() {
		Mongo.replaceOne(String.format("fews.%s", getClass().getSimpleName()), new Document(), Fews.getParameters(), new ReplaceOptions().upsert(true));
	}

	@Override
	public String[] getPredecessors() {
		return predecessors;
	}
}
