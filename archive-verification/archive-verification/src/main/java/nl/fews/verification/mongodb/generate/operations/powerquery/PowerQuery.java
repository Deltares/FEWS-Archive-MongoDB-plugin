package nl.fews.verification.mongodb.generate.operations.powerquery;

import nl.fews.verification.mongodb.generate.shared.execute.Execute;
import nl.fews.verification.mongodb.generate.shared.graph.Graph;
import nl.fews.verification.mongodb.shared.database.Mongo;
import org.bson.Document;

public class PowerQuery {
	private PowerQuery(){}

	public static void execute(){
		Mongo.deleteMany("output.PowerQuery", new Document());
		Mongo.find("Study", new Document("Active", true)).forEach(study ->
			Graph.getDirectedAcyclicGraphGroups(Graph.getDirectedAcyclicGraph(PowerQuery.class, new Object[]{study.getString("Name")})).forEach(Execute::execute));
	}
}
