package io.ctsi.tenet.kafka.connect.json;

import io.ctsi.tenet.kafka.connect.storage.ConverterConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.EnumMap;
import java.util.Map;

public class JsonConverterConfig extends ConverterConfig {

    public static final String SCHEMAS_ENABLE_CONFIG = "schemas.enable";
    public  static final String  TABLE_IN_SUFFIX = "_in";
    public  static final String  TABLE_OUT_SUFFIX = "_out";
    public  static final String  OPR_TYPE = "oprType";
    public static final boolean SCHEMAS_ENABLE_DEFAULT = true;
    private static final String SCHEMAS_ENABLE_DOC = "Include schemas within each of the serialized values and keys.";
    private static final String SCHEMAS_ENABLE_DISPLAY = "Enable Schemas";
    public static enum OprType{
        INPUT,OUTPUT;
    }
    private final static Map<OprType, String> OPR_TYPE_MAP = new EnumMap<>(OprType.class);


    public static final String SCHEMAS_CACHE_SIZE_CONFIG = "schemas.cache.size";
    //缓存schema大小，map 64足够了
    public static final int SCHEMAS_CACHE_SIZE_DEFAULT = 64;
    private static final String SCHEMAS_CACHE_SIZE_DOC = "The maximum number of schemas that can be cached in this converter instance.";
    private static final String SCHEMAS_CACHE_SIZE_DISPLAY = "Schema Cache Size";

    private final static ConfigDef CONFIG;

    static {
        //TODO
        OPR_TYPE_MAP.put(OprType.INPUT,TABLE_IN_SUFFIX);
        OPR_TYPE_MAP.put(OprType.OUTPUT,TABLE_OUT_SUFFIX);
        String group = "Schemas";
        int orderInGroup = 0;
        CONFIG = ConverterConfig.newConfigDef();
        CONFIG.define(SCHEMAS_ENABLE_CONFIG, ConfigDef.Type.BOOLEAN, SCHEMAS_ENABLE_DEFAULT, ConfigDef.Importance.HIGH, SCHEMAS_ENABLE_DOC, group,
                orderInGroup++, ConfigDef.Width.MEDIUM, SCHEMAS_ENABLE_DISPLAY);
        CONFIG.define(SCHEMAS_CACHE_SIZE_CONFIG, ConfigDef.Type.INT, SCHEMAS_CACHE_SIZE_DEFAULT, ConfigDef.Importance.HIGH, SCHEMAS_CACHE_SIZE_DOC, group,
                orderInGroup++, ConfigDef.Width.MEDIUM, SCHEMAS_CACHE_SIZE_DISPLAY);
    }

    public static ConfigDef configDef() {
        return CONFIG;
    }

    public JsonConverterConfig(Map<String, ?> props) {
        super(CONFIG, props);
    }

    /**
     * Return whether schemas are enabled.
     *
     * @return true if enabled, or false otherwise
     */
    public boolean schemasEnabled() {
        return getBoolean(SCHEMAS_ENABLE_CONFIG);
    }

    /**
     * Get the cache size.
     *
     * @return the cache size
     */
    public int schemaCacheSize() {
        return getInt(SCHEMAS_CACHE_SIZE_CONFIG);
    }

    public static String getTableSuffix(OprType k){
        return OPR_TYPE_MAP.get(k);
    }

}

