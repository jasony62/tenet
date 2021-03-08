package io.ctsi.tenet.kafka.config;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.core.env.Environment;
import org.springframework.util.Assert;
import org.springframework.util.ResourceUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;

/**
 * 用于数据库配置
 */
@ConfigurationProperties(prefix = "tenet.schema")
public class SchemaDescProperties {
    public static final String LOCATION_CONFIG = "spring.config.location";

    @Autowired
    private Environment env;

    private String filePath;

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }


    private JsonNode buildProperties() {

        Assert.hasLength(this.getFilePath(), "初始化参数失败，配置文件中的schema不能为空");
        String pathPrefix = "classpath:";
        if (env.getProperty(LOCATION_CONFIG) != null) {
            pathPrefix = env.getProperty(LOCATION_CONFIG);
        }
        File jsonFile = null;
        try {
            jsonFile = ResourceUtils.getFile(pathPrefix + this.getFilePath());
        } catch (FileNotFoundException e) {
            Assert.notNull(jsonFile, "schema配置文件没有找到");
        }
        ObjectMapper jsonMapper = new ObjectMapper();
        JsonNode rootNode = null;
        try {
            rootNode = jsonMapper.readTree(jsonFile);
        } catch (IOException e) {
            Assert.notNull(rootNode, "schema配置文件格式错误");
        }
        //System.out.println("schema-config:" + rootNode.toPrettyString());
        return rootNode;

    }

    public JsonNode getInputJsonNode() {
        JsonNode node = buildProperties().get("in_topics");
        return adapterNode(node,"_in");
    }

    public JsonNode getOutputJsonNode() {
        JsonNode node = buildProperties().get("out_topic");
        return adapterNode(node,"_out");
    }

    private JsonNode adapterNode(JsonNode node,String suffix){

        Iterator<String> iter = node.fieldNames();
        ObjectNode objectNode = JsonNodeFactory.instance.objectNode();
        while (iter.hasNext()) {
            String fieldName = iter.next();
            objectNode.set(fieldName+suffix,node.get(fieldName));
        }
        return objectNode;

    }

}
