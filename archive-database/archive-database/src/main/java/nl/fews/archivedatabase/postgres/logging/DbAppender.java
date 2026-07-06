package nl.fews.archivedatabase.postgres.logging;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import nl.fews.archivedatabase.common.shared.database.Collection;
import nl.fews.archivedatabase.postgres.database.DatabaseUtil;
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
import org.json.JSONObject;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Timestamp;
import java.util.Date;
import java.util.List;
import java.util.Map;

@Plugin(name = "PostgresAppender", category = Core.CATEGORY_NAME, elementType = Appender.ELEMENT_TYPE)
public class DbAppender extends AbstractAppender {

	/**
	 *
	 */
	private final HikariDataSource dataSource;

	/**
	 *
	 */
	private final String table;

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
		var config = new HikariConfig();
		config.setJdbcUrl(connectionString);
		config.setMaximumPoolSize(32);
		config.setMinimumIdle(2);
		config.setPoolName("postgres-log-pool");
		config.setConnectionTimeout(30 * 1000);
		config.setIdleTimeout(10 * 60 * 1000);
		config.setMaxLifetime(30 * 60 * 1000);

		dataSource = new HikariDataSource(config);

		this.table = Collection.MigrateLog.toString();
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
		if(event.getLoggerName() != null && event.getLoggerName().startsWith("TODO: IF NEEDED OR REMOVE"))
			return;

		Map<String, Object> document = addExtraMessage(new JSONObject(jsonLayout.toSerializable(event)).toMap());
		document.put("date", new Date());
		document.put("machineName", this.machine);

		var params = List.of(
			new Timestamp(((Date)document.get("date")).getTime()),
			document.getOrDefault("errorCategory", ""),
			new JSONObject(document).toString());

		var sql = String.format("INSERT INTO \"%s\" (\"date\", \"errorCategory\", \"data\") VALUES (?, ?, ?::jsonb)", table);

		try (var c = dataSource.getConnection(); var ps = c.prepareStatement(sql)) {
			DatabaseUtil.bindParams(ps, params);
			ps.execute();
		}
		catch (Exception ex){
			error(ex.getMessage(), ex);
		}
	}

	/**
	 *
	 * @param document document
	 */
	private Map<String, Object> addExtraMessage(Map<String, Object> document){
		try{
			Map<String, Object> extra = new JSONObject((String)document.get("message")).toMap();
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
        if (dataSource != null) {
            dataSource.close();
        }
    }
}
