package bg.government.opendatapusher;

import java.util.List;

public class PushConfig {

    private String title;
    private SourceType sourceType;
    private String path;
    private String query;
    private String connectionString;
    private int days;
    private String datasetKey;
    
    public SourceType getSourceType() {
        return sourceType;
    }
    public void setSourceType(SourceType sourceType) {
        this.sourceType = sourceType;
    }
    public String getPath() {
        return path;
    }
    public void setPath(String path) {
        this.path = path;
    }
    public String getQuery() {
        return query;
    }
    public void setQuery(String query) {
        this.query = query;
    }
    public String getConnectionString() {
        return connectionString;
    }
    public void setConnectionString(String connectionString) {
        this.connectionString = connectionString;
    }
    public int getDays() {
        return days;
    }
    public void setDays(int days) {
        this.days = days;
    }
    public String getTitle() {
        return title;
    }
    public void setTitle(String title) {
        this.title = title;
    }
    
    public String getDatasetKey() {
        return datasetKey;
    }
    public void setDatasetKey(String datasetKey) {
        this.datasetKey = datasetKey;
    }


    public static class ConfigRoot {
        private List<PushConfig> configs;
        private String apiKey;

        public List<PushConfig> getConfigs() {
            return configs;
        }
        public void setConfigs(List<PushConfig> configs) {
            this.configs = configs;
        }
        public String getApiKey() {
            return apiKey;
        }
        public void setApiKey(String apiKey) {
            this.apiKey = apiKey;
        }
    }
}
