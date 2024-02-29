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
public class LocationAttributes {
	@QueryMapping
	public Document locationAttributesById(@Argument String _id, DataFetchingEnvironment e){
		return  Mongo.findOne("LocationAttributes", new Document("_id", new ObjectId(_id)), Conversion.getProjection(e));
	}

	@QueryMapping
	public Document locationAttributesByName(@Argument String name, DataFetchingEnvironment e){
		return  Mongo.findOne("LocationAttributes", new Document("Name", name), Conversion.getProjection(e));
	}

	@QueryMapping
	public List<Document> locationAttributesN(DataFetchingEnvironment e){
		return StreamSupport.stream(Mongo.find("LocationAttributes", new Document(), Conversion.getProjection(e)).spliterator(), false).collect(Collectors.toList());
	}
	
	@MutationMapping
	public String createLocationAttributes(@Argument Map<String, Object> document){
		return Mongo.insertOne("LocationAttributes", new Document(document)).getInsertedId().asObjectId().getValue().toString();
	}

	@MutationMapping
	public Long updateLocationAttributes(@Argument String _id, @Argument Map<String, Object> document){
		return Mongo.updateOne("LocationAttributes", new Document("_id", new ObjectId(_id)), new Document(document)).getModifiedCount();
	}

	@MutationMapping
	public Long deleteLocationAttributes(@Argument String _id){
		return Mongo.deleteOne("LocationAttributes", new Document("_id", new ObjectId(_id))).getDeletedCount();
	}
}