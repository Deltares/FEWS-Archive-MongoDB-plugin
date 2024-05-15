package nl.fews.verification.mongodb.web.controllers.verification;

import graphql.schema.DataFetchingEnvironment;
import nl.fews.verification.mongodb.shared.database.Mongo;
import nl.fews.verification.mongodb.web.shared.conversion.Conversion;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.graphql.data.method.annotation.Argument;
import org.springframework.graphql.data.method.annotation.MutationMapping;
import org.springframework.graphql.data.method.annotation.QueryMapping;
import org.springframework.stereotype.Controller;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@Controller
public class OutputView {
	@QueryMapping
	public Document outputViewById(@Argument String _id, DataFetchingEnvironment e){
		return Mongo.findOne("output.View", new Document("_id", new ObjectId(_id)), Conversion.getProjection(e));
	}
	
	@QueryMapping
	public Document outputViewByDatabaseStateView(@Argument String database, @Argument String state, @Argument String view, DataFetchingEnvironment e){
		return Mongo.findOne("output.View", new Document("Database", database).append("State", state).append("View", view), Conversion.getProjection(e));
	}

	@QueryMapping
	public List<Document> outputViewNByStateEnvironmentStudy(@Argument String state, @Argument String environment, @Argument String study, DataFetchingEnvironment e){
		return StreamSupport.stream(Mongo.find("output.View", new Document("State", state).append("Environment", environment).append("Study", study), Conversion.getProjection(e)).sort(new Document("Name", 1)).spliterator(), false).collect(Collectors.toList());
	}

	@QueryMapping
	public List<Document> outputViewNByStateEnvironment(@Argument String state, @Argument String environment, DataFetchingEnvironment e){
		return StreamSupport.stream(Mongo.find("output.View", new Document("State", state).append("Environment", environment), Conversion.getProjection(e)).sort(new Document("Name", 1)).spliterator(), false).collect(Collectors.toList());
	}

	@QueryMapping
	public List<Document> outputViewN(DataFetchingEnvironment e){
		return StreamSupport.stream(Mongo.find("output.View", new Document(), Conversion.getProjection(e)).sort(new Document("Name", 1)).spliterator(), false).collect(Collectors.toList());
	}
	
	@MutationMapping
	public String createOutputView(@Argument String database, @Argument String state, @Argument String view, @Argument String collection, @Argument String name, @Argument String environment, @Argument String study, @Argument Map<String, Object> value){
		return Mongo.insertOne("output.View", new Document("Database", database).append("State", state).append("View", view).append("Collection", collection).append("Name", name).append("Environment", environment).append("Study", study).append("Value", value)).getInsertedId().asObjectId().getValue().toString();
	}

	@MutationMapping
	public Long updateOutputView(@Argument String _id, @Argument String database, @Argument String state, @Argument String view, @Argument String collection, @Argument String name, @Argument String environment, @Argument String study, @Argument Map<String, Object> value){
		return Mongo.updateOne("output.View", new Document("_id", new ObjectId(_id)), new Document("$set", new Document("Database", database).append("State", state).append("View", view).append("Collection", collection).append("Name", name).append("Environment", environment).append("Study", study).append("Value", value))).getModifiedCount();
	}

	@MutationMapping
	public Long deleteOutputView(@Argument String _id){
		return Mongo.deleteOne("output.View", new Document("_id", new ObjectId(_id))).getDeletedCount();
	}
}
