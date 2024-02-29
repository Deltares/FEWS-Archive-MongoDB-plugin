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
public class Study {
	@QueryMapping
	public Document studyById(@Argument String _id, DataFetchingEnvironment e){
		return  Mongo.findOne("Study", new Document("_id", new ObjectId(_id)), Conversion.getProjection(e));
	}

	@QueryMapping
	public Document studyByName(@Argument String name, DataFetchingEnvironment e){
		return  Mongo.findOne("Study", new Document("Name", name), Conversion.getProjection(e));
	}

	@QueryMapping
	public List<Document> studyN(DataFetchingEnvironment e){
		return StreamSupport.stream(Mongo.find("Study", new Document(), Conversion.getProjection(e)).spliterator(), false).collect(Collectors.toList());
	}
	
	@MutationMapping
	public String createStudy(@Argument Map<String, Object> document){
		return Mongo.insertOne("Study", new Document(document)).getInsertedId().asObjectId().getValue().toString();
	}

	@MutationMapping
	public Long updateStudy(@Argument String _id, @Argument Map<String, Object> document){
		return Mongo.updateOne("Study", new Document("_id", new ObjectId(_id)), new Document(document)).getModifiedCount();
	}

	@MutationMapping
	public Long deleteStudy(@Argument String _id){
		return Mongo.deleteOne("Study", new Document("_id", new ObjectId(_id))).getDeletedCount();
	}
}
