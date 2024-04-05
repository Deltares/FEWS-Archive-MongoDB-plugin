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

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@Controller
public class OutputCube {
	@QueryMapping
	public Document outputCubeById(@Argument String _id, DataFetchingEnvironment e){
		return Mongo.findOne("output.Cube", new Document("_id", new ObjectId(_id)), Conversion.getProjection(e));
	}

	@QueryMapping
	public Document outputCubeByName(@Argument String name, DataFetchingEnvironment e){
		return Mongo.findOne("output.Cube", new Document("Name", name), Conversion.getProjection(e));
	}

	@QueryMapping
	public List<Document> outputCubeN(DataFetchingEnvironment e){
		return StreamSupport.stream(Mongo.find("output.Cube", new Document(), Conversion.getProjection(e)).spliterator(), false).collect(Collectors.toList());
	}
	
	@MutationMapping
	public String createOutputCube(@Argument String name, @Argument Map<String, Object> bim){
		return Mongo.insertOne("output.Cube", new Document("Name", name).append("Bim", bim)).getInsertedId().asObjectId().getValue().toString();
	}

	@MutationMapping
	public Long updateOutputCube(@Argument String _id, @Argument String name, @Argument Map<String, Object> bim){
		return Mongo.updateOne("output.Cube", new Document("_id", new ObjectId(_id)), new Document("$set", new Document("Name", name).append("Bim", bim))).getModifiedCount();
	}

	@MutationMapping
	public Long deleteOutputCube(@Argument String _id){
		return Mongo.deleteOne("output.Cube", new Document("_id", new ObjectId(_id))).getDeletedCount();
	}
}
