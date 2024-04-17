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
public class ConfigurationDescription {
	@QueryMapping
	public Document configurationDescriptionById(@Argument String _id, DataFetchingEnvironment e){
		return Mongo.findOne("configuration.Description", new Document("_id", new ObjectId(_id)), Conversion.getProjection(e));
	}

	@QueryMapping
	public Document configurationDescriptionByEnvironment(@Argument String name, DataFetchingEnvironment e){
		return Mongo.findOne("configuration.Description", new Document("Name", name), Conversion.getProjection(e));
	}

	@QueryMapping
	public List<Document> configurationDescriptionN(DataFetchingEnvironment e){
		return StreamSupport.stream(Mongo.find("configuration.Description", new Document(), Conversion.getProjection(e)).sort(new Document("Name", 1)).spliterator(), false).collect(Collectors.toList());
	}

	@MutationMapping
	public String createConfigurationDescription(
			@Argument String name,
			@Argument String toEmailAddresses,
			@Argument String fromEmailAddress,
			@Argument String drdlYamlPath,
			@Argument String environment,
			@Argument String threads,
			@Argument String cubeAdmins,
			@Argument String cubeUsers,
			@Argument String bimPath,
			@Argument String databaseConnectionString,
			@Argument String smtpServer,
			@Argument String tabularConnectionString,
			@Argument String drdlYamlServiceRestart,
			@Argument String execute,
			@Argument String archiveDb,
			@Argument String taskInterval,
			@Argument String databaseConnectionAesPassword,
			@Argument String databaseConnectionUsername,
			@Argument String parallelPartitions,
			@Argument String drdlYamlConfigPath,
			@Argument String drdlYamlMongoDbUri){
		return Mongo.insertOne("configuration.Description",
				new Document("Name", name)
				.append("toEmailAddresses", toEmailAddresses)
				.append("fromEmailAddress", fromEmailAddress)
				.append("drdlYamlPath", drdlYamlPath)
				.append("environment", environment)
				.append("threads", threads)
				.append("cubeAdmins", cubeAdmins)
				.append("cubeUsers", cubeUsers)
				.append("bimPath", bimPath)
				.append("databaseConnectionString", databaseConnectionString)
				.append("smtpServer", smtpServer)
				.append("tabularConnectionString", tabularConnectionString)
				.append("drdlYamlServiceRestart", drdlYamlServiceRestart)
				.append("execute", execute)
				.append("archiveDb", archiveDb)
				.append("taskInterval", taskInterval)
				.append("databaseConnectionAesPassword", databaseConnectionAesPassword)
				.append("databaseConnectionUsername", databaseConnectionUsername)
				.append("parallelPartitions", parallelPartitions)
				.append("drdlYamlConfigPath", drdlYamlConfigPath)
				.append("drdlYamlMongoDbUri", drdlYamlMongoDbUri)
		).getInsertedId().asObjectId().getValue().toString();
	}

	@MutationMapping
	public Long updateConfigurationDescription(
			@Argument String _id,
			@Argument String name,
			@Argument String toEmailAddresses,
			@Argument String fromEmailAddress,
			@Argument String drdlYamlPath,
			@Argument String environment,
			@Argument String threads,
			@Argument String cubeAdmins,
			@Argument String cubeUsers,
			@Argument String bimPath,
			@Argument String databaseConnectionString,
			@Argument String smtpServer,
			@Argument String tabularConnectionString,
			@Argument String drdlYamlServiceRestart,
			@Argument String execute,
			@Argument String archiveDb,
			@Argument String taskInterval,
			@Argument String databaseConnectionAesPassword,
			@Argument String databaseConnectionUsername,
			@Argument String parallelPartitions,
			@Argument String drdlYamlConfigPath,
			@Argument String drdlYamlMongoDbUri){
		return Mongo.updateOne("configuration.Description", new Document("_id", new ObjectId(_id)), new Document("$set",
				new Document("Name", name)
				.append("toEmailAddresses", toEmailAddresses)
				.append("fromEmailAddress", fromEmailAddress)
				.append("drdlYamlPath", drdlYamlPath)
				.append("environment", environment)
				.append("threads", threads)
				.append("cubeAdmins", cubeAdmins)
				.append("cubeUsers", cubeUsers)
				.append("bimPath", bimPath)
				.append("databaseConnectionString", databaseConnectionString)
				.append("smtpServer", smtpServer)
				.append("tabularConnectionString", tabularConnectionString)
				.append("drdlYamlServiceRestart", drdlYamlServiceRestart)
				.append("execute", execute)
				.append("archiveDb", archiveDb)
				.append("taskInterval", taskInterval)
				.append("databaseConnectionAesPassword", databaseConnectionAesPassword)
				.append("databaseConnectionUsername", databaseConnectionUsername)
				.append("parallelPartitions", parallelPartitions)
				.append("drdlYamlConfigPath", drdlYamlConfigPath)
				.append("drdlYamlMongoDbUri", drdlYamlMongoDbUri)
			)).getModifiedCount();
	}

	@MutationMapping
	public Long deleteConfigurationDescription(@Argument String _id){
		return Mongo.deleteOne("configuration.Description", new Document("_id", new ObjectId(_id))).getDeletedCount();
	}
}
