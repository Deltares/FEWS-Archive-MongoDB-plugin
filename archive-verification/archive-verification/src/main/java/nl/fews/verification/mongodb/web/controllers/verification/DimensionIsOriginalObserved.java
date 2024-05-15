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
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@Controller
public class DimensionIsOriginalObserved {
	@QueryMapping
	public Document dimensionIsOriginalObservedById(@Argument String _id, DataFetchingEnvironment e){
		return Mongo.findOne("dimension.IsOriginalObserved", new Document("_id", new ObjectId(_id)), Conversion.getProjection(e));
	}
	
	@QueryMapping
	public List<Document> dimensionIsOriginalObservedN(DataFetchingEnvironment e){
		return StreamSupport.stream(Mongo.find("dimension.IsOriginalObserved", new Document(), Conversion.getProjection(e)).sort(new Document("isOriginalObserved", 1)).spliterator(), false).collect(Collectors.toList());
	}
	
	@MutationMapping
	public String createDimensionIsOriginalObserved(@Argument String isOriginalObserved){
		return Mongo.insertOne("dimension.IsOriginalObserved", new Document("IsOriginalObserved", isOriginalObserved)).getInsertedId().asObjectId().getValue().toString();
	}

	@MutationMapping
	public Long updateDimensionIsOriginalObserved(@Argument String _id, @Argument String isOriginalObserved){
		return Mongo.updateOne("dimension.IsOriginalObserved", new Document("_id", new ObjectId(_id)), new Document("$set", new Document("isOriginalObserved", isOriginalObserved))).getModifiedCount();
	}

	@MutationMapping
	public Long deleteDimensionIsOriginalObserved(@Argument String _id){
		return Mongo.deleteOne("dimension.IsOriginalObserved", new Document("_id", new ObjectId(_id))).getDeletedCount();
	}
}
