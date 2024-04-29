package nl.fews.verification.mongodb.generate.operations.powerquerysql;

import nl.fews.verification.mongodb.generate.shared.execute.Execute;
import nl.fews.verification.mongodb.generate.shared.graph.Graph;
import nl.fews.verification.mongodb.shared.database.Mongo;
import org.bson.Document;

public class PowerQuerySql {
	private PowerQuerySql(){}

	public static void execute(){
		Mongo.deleteMany("output.PowerQuerySql", new Document());
		Mongo.find("Study", new Document("Active", true)).forEach(study ->
			Graph.getDirectedAcyclicGraphGroups(Graph.getDirectedAcyclicGraph(PowerQuerySql.class, new Object[]{study.getString("Name")})).forEach(Execute::execute));
	}
}
