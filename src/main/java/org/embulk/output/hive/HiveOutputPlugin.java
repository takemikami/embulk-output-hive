package org.embulk.output.hive;

import java.io.File;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.embulk.config.TaskReport;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.spi.*;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.type.*;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class HiveOutputPlugin
        implements OutputPlugin
{
    public interface PluginTask
            extends Task
    {
        // configuration url (required String)
        @Config("url")
        public String getUrl();

        // configuration user (required String)
        @Config("user")
        public String getUser();

        // configuration password (required String)
        @Config("password")
        public String getPassword();

        // configuration database (required String)
        @Config("database")
        public String getDatabase();

        // configuration table (required String)
        @Config("table")
        public String getTable();

        // configuration config_files (required List<String>)
        @Config("config_files")
        @ConfigDefault("[]")
        List<String> getConfigFiles();

        // configuration config (required Map<String,String>)
        @Config("config")
        @ConfigDefault("{}")
        Map<String, String> getConfig();

        // configuration path_prefix (required String)
        @Config("location")
        public String getLocation();
    }

    @Override
    public ConfigDiff transaction(ConfigSource config,
            Schema schema, int taskCount,
            OutputPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);

        // retryable (idempotent) output:
        // return resume(task.dump(), schema, taskCount, control);
        control.run(task.dump());

        StringBuffer buff = new StringBuffer();
        for (Column c : schema.getColumns()) {
            String hiveType = "string";
            if (c.getType() instanceof BooleanType) {
                hiveType = "boolean";
            } else if (c.getType() instanceof LongType) {
                hiveType = "int";
            } else if (c.getType() instanceof DoubleType) {
                hiveType = "double";
            } else if (c.getType() instanceof TimestampType) {
                hiveType = "timestamp";
            }
            if(buff.length()>0) { buff.append(","); }
            buff.append(c.getName()).append(" ").append(hiveType);
        }

        try {
            String tableName = task.getTable();
            Class.forName("org.apache.hive.jdbc.HiveDriver");
            Connection con = DriverManager.getConnection(task.getUrl(), task.getUser(), task.getPassword());
            Statement stmt = con.createStatement();
            stmt.execute("use " + task.getDatabase());
            stmt.execute("drop table if exists " + tableName);
            stmt.execute("create external table " + tableName + " (" + buff.toString() + ")"
                    + " row format delimited fields terminated by '\\t' lines terminated by '\\n'"
                    + " stored as textfile location '" + task.getLocation() + "'"
            );
        } catch (Exception ex) {
            ex.printStackTrace();
            System.exit(1);
        }

        return Exec.newConfigDiff();
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource,
            Schema schema, int taskCount,
            OutputPlugin.Control control)
    {
        throw new UnsupportedOperationException("hive output plugin does not support resuming");
    }

    @Override
    public void cleanup(TaskSource taskSource,
            Schema schema, int taskCount,
            List<TaskReport> successTaskReports)
    {
    }

    @Override
    public TransactionalPageOutput open(TaskSource taskSource, Schema schema, int taskIndex)
    {
        PluginTask task = taskSource.loadTask(PluginTask.class);
        PageReader reader = new PageReader(schema);
        PluginPageOutput output = new PluginPageOutput(reader, schema, task);
        return output;
    }


    public static class PluginPageOutput
            implements TransactionalPageOutput {
        private static DateTimeFormatter TO_STRING_FORMATTER_MILLIS = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS ").withZoneUTC();
        private PageReader reader = null;
        private Schema schema = null;
        private PluginTask task = null;
        PluginPageOutput(PageReader reader, Schema schema, PluginTask task) {
            this.reader = reader;
            this.schema = schema;
            this.task = task;
        }
        public void add(Page page) {
            reader.setPage(page);
            StringBuffer buff = new StringBuffer();
            while (reader.nextRecord()) {
                for (Column c : schema.getColumns()) {
                    if (c.getIndex() > 0) {
                        buff.append("\t");
                    }
                    if (c.getType() instanceof BooleanType) {
                        buff.append(reader.getBoolean(c));
                    } else if (c.getType() instanceof LongType) {
                        buff.append(reader.getLong(c));
                    } else if (c.getType() instanceof DoubleType) {
                        buff.append(reader.getDouble(c));
                    } else if (c.getType() instanceof TimestampType) {
                        Timestamp v = reader.getTimestamp(c);
                        buff.append(TO_STRING_FORMATTER_MILLIS.print(v.toEpochMilli()));
                    } else if (c.getType() instanceof JsonType) {
                        buff.append(reader.getJson(c));
                    } else if (c.getType() instanceof StringType) {
                        buff.append(reader.getString(c));
                    }
                }
                buff.append("\n");
            }

            // upload to hdfs
            try {
                Configuration configuration = new Configuration();
                for (String configFile : task.getConfigFiles()) {
                    File file = new File(configFile);
                    configuration.addResource(file.toURI().toURL());
                }
                for (Map.Entry<String, String> entry: task.getConfig().entrySet()) {
                    configuration.set(entry.getKey(), entry.getValue());
                }
                Path hdfsPath = new Path(task.getLocation() + "/00000");
                FileSystem fs = hdfsPath.getFileSystem(configuration);
                OutputStream output = fs.create(new Path(task.getLocation() + "/00000"), false);
                output.write(buff.toString().getBytes());
                output.close();

            } catch(Exception ex) {
                ex.printStackTrace();
                System.exit(1);
            }

        }
        public void finish() {}
        public void close() {}
        public void abort() {}
        public TaskReport commit() { return Exec.newTaskReport(); }
    }
}
