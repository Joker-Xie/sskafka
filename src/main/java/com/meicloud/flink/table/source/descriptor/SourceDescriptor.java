package com.meicloud.flink.table.source.descriptor;

import com.meicloud.spark.entity.CaseVo.*;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class SourceDescriptor implements Serializable {
    private static final long serialVersionUID = -3166370111211057517L;
    private Map<String, Object> conf;
    private Properties properties;

    public SourceDescriptor() {
        this.conf = new HashMap();
        properties = new Properties();
    }

    public Map<String, Object> getConf() {
        return conf;
    }

    public void setConf(Map<String, Object> conf) {
        this.conf = conf;
    }

    public SourceDescriptor setProperties(String key, String value) {
        properties.put(key, value);
        conf.put("general_props", properties);
        return this;
    }


    public SourceDescriptor setSourceType(String sourceType) {
        conf.put("source_type", sourceType);
        return this;
    }


    public SourceDescriptor setDataType(String dataType) {
        conf.put("data_type", dataType);
        return this;
    }


    public SourceDescriptor setStartupMode(String startupMode) {
        conf.put("startup_mode", startupMode);
        return this;
    }

    public Properties getProperties() {
        return properties;
    }

    public String getSourceType() {
        return conf.get("source_type").toString();
    }

    public String getDataType() {
        return conf.get("data_type").toString();
    }

    public String getStartupMode() {
        return conf.get("startup_mode").toString();
    }
}
