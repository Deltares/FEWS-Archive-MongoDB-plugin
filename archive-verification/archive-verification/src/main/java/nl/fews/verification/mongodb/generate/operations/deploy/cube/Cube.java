package nl.fews.verification.mongodb.generate.operations.deploy.cube;

import nl.fews.verification.mongodb.generate.interfaces.IExecute;
import nl.fews.verification.mongodb.generate.interfaces.IPredecessor;
import nl.fews.verification.mongodb.shared.database.Mongo;
import nl.fews.verification.mongodb.shared.io.IO;
import nl.fews.verification.mongodb.shared.settings.Settings;
import org.bson.Document;
import org.bson.json.JsonWriterSettings;

import java.nio.file.Path;

public final class Cube implements IExecute, IPredecessor {

	private final String[] predecessors = new String[]{};
	private final String study;

	public Cube(String study){
		this.study = study;
	}

	/**
	 * Executes the method by writing a string to a file specified by the given path.
	 * The string to be written is obtained by retrieving the "Bim" field of the "output.Cube" document
	 * from the "Verification" collection in the MongoDB database. The filter used to retrieve the document
	 * is based on the "Name" field matching the provided study string.
	 * The retrieved string is then written to the file specified by the concatenation of the "bimPath" setting
	 * and the study string formatted as "Verification_{study}.bim". The write operation uses the writeString
	 * method from the IO class.
	 *
	 * @throws RuntimeException if an error occurs while writing the string to the file
	 */
	@Override
	public void execute(){
		IO.writeString(Path.of(Settings.get("bimPath"), String.format("Verification_%s.bim", study)), Mongo.findOne("output.Cube", new Document("Name", study)).get("Bim", Document.class).toJson(JsonWriterSettings.builder().indent(true).build()));
	}

	/**
	 * Retrieves the predecessors of the current object.
	 *
	 * @return an array of strings containing the predecessors of the current object
	 */
	@Override
	public String[] getPredecessors() {
		return predecessors;
	}
}
