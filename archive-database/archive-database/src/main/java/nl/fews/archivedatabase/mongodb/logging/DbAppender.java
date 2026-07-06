package nl.fews.archivedatabase.mongodb.logging;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import nl.fews.archivedatabase.common.shared.database.Collection;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Core;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.core.layout.JsonLayout;
import org.bson.Document;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;

@Plugin(name = "MongoDbAppender", category = Core.CATEGORY_NAME, elementType = Appender.ELEMENT_TYPE)
public class DbAppender extends AbstractAppender {

	/**
	 *
	 */
	private final MongoClient client;

	/**
	 *
	 */
	private final String collection;

	/**
	 *
	 */
	private final String database;

	/**
	 *
	 */
	private final String machine;

	/**
	 *
	 */
	private final JsonLayout jsonLayout;

	/**
	 *
	 * @param name name
	 * @param filter filter
	 */
	protected DbAppender(String name, Filter filter, String connectionString) {
		super(name, filter, JsonLayout.createDefaultLayout(), true, null);
		ConnectionString connection = new ConnectionString(connectionString);
        this.client = MongoClients.create(connection);
		this.collection = Collection.MigrateLog.toString();
        this.database = connection.getDatabase();
		this.jsonLayout = (JsonLayout)getLayout();

		String machine;
        try {
            machine = InetAddress.getLocalHost().getHostName();
        }
		catch (UnknownHostException e) {
			machine = "";
        }
		this.machine = machine;
	}

	/**
	 *
	 * @param name name
	 * @param connectionString connectionString
	 * @param filter filter
	 * @return MongoDbAppender
	 */
	@PluginFactory
	public static DbAppender createAppender(
			@PluginAttribute("name") String name,
			@PluginAttribute("connectionString") String connectionString,
			@PluginElement("Filter") Filter filter) {
		return new DbAppender(name, filter, connectionString);
	}

	/**
	 *
	 * @param event event
	 */
	@Override
	public void append(LogEvent event) {
		if(event.getLoggerName() != null && event.getLoggerName().startsWith("org.mongodb."))
			return;
		try{
			Document document = addExtraMessage(Document.parse(jsonLayout.toSerializable(event)));
			document.append("date", new Date());
			document.append("machineName", this.machine);
			client.getDatabase(database).getCollection(collection).insertOne(document);
		}
		catch (Exception ex){
			error(ex.getMessage(), ex);
		}
	}

	/**
	 *
	 * @param document document
	 */
	private Document addExtraMessage(Document document){
		try{
			Document extra = Document.parse(document.getString("message"));
			document.putAll(extra);
			if(!extra.containsKey("message"))
				document.remove("message");
		}
		catch (Exception ex){
			//IGNORE
		}
		return document;
	}

	@Override
    public void stop() {
        super.stop();
        if (client != null) {
            client.close();
        }
    }
}
