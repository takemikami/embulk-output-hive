Embulk::JavaPlugin.register_output(
  "hive", "org.embulk.output.hive.HiveOutputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
