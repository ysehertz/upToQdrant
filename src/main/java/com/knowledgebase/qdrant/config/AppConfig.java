package com.knowledgebase.qdrant.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * 应用程序配置类，负责加载和管理应用程序配置
 */
public class AppConfig {
    private static final Logger logger = LoggerFactory.getLogger(AppConfig.class);
    private final Properties properties = new Properties();

    public AppConfig() {
        loadProperties();
    }

    private void loadProperties() {
        try (InputStream input = getClass().getClassLoader().getResourceAsStream("application.properties")) {
            if (input == null) {
                logger.error("无法找到 application.properties 文件");
                throw new RuntimeException("无法找到 application.properties 文件");
            }
            properties.load(input);
            logger.info("成功加载配置文件");
        } catch (IOException e) {
            logger.error("加载配置文件时发生错误", e);
            throw new RuntimeException("加载配置文件失败", e);
        }
    }

    public String getQdrantUrl() {
        return properties.getProperty("qdrant.url");
    }

    public String getQdrantApiKey() {
        return properties.getProperty("qdrant.api.key");
    }

    public String getQdrantCollectionName() {
        return properties.getProperty("qdrant.collection.name");
    }

    public int getQdrantVectorSize() {
        return Integer.parseInt(properties.getProperty("qdrant.vector.size", "1536"));
    }

    public String getOpenAiApiKey() {
        return properties.getProperty("openai.api.key");
    }

    public String getOpenAiEmbeddingModel() {
        return properties.getProperty("openai.embedding.model");
    }

    public Path getKnowledgeBaseDirectory() {
        return Paths.get(properties.getProperty("knowledge.base.directory"));
    }

    public List<String> getKnowledgeBaseExtensions() {
        String extensions = properties.getProperty("knowledge.base.extensions", "txt,md");
        return Arrays.asList(extensions.split(","));
    }

    public String getUpdateCron() {
        return properties.getProperty("update.cron");
    }

    public int getUploadBatchSize() {
        return Integer.parseInt(properties.getProperty("upload.batch_size", "100"));
    }

    public int getUploadChunkSize() {
        return Integer.parseInt(properties.getProperty("upload.chunk_size", "1000"));
    }
} 