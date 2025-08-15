package com.knowledgebase.qdrant;

import com.knowledgebase.qdrant.config.AppConfig;
import com.knowledgebase.qdrant.scheduler.UpdateScheduler;
import com.knowledgebase.qdrant.service.QdrantService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * 应用程序主类，负责初始化和启动知识库上传服务
 */
public class QdrantUploaderApplication {
    private static final Logger logger = LoggerFactory.getLogger(QdrantUploaderApplication.class);

    public static void main(String[] args) {
        try {
            logger.info("启动知识库上传服务...");
            
            // 初始化配置
            AppConfig appConfig = new AppConfig();
            
            // 确保日志目录存在
            ensureLogDirectoryExists();
            
            // 初始化Qdrant服务
            QdrantService qdrantService = new QdrantService(appConfig);
            
            // 确保集合存在
            qdrantService.ensureCollectionExists();
            
            // 设置并启动更新调度器
            UpdateScheduler scheduler = new UpdateScheduler(qdrantService, appConfig);
            scheduler.start();
            
            // 执行初始上传
            qdrantService.syncKnowledgeBase();
            
            logger.info("知识库上传服务启动成功，将按计划执行定期更新");
            
            // 添加关闭钩子，确保应用程序正常关闭时清理资源
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.info("应用程序正在关闭...");
                scheduler.stop();
                qdrantService.close();
                logger.info("应用程序已安全关闭");
            }));
        } catch (Exception e) {
            logger.error("启动服务失败", e);
            System.exit(1);
        }
    }
    
    /**
     * 确保日志目录存在
     */
    private static void ensureLogDirectoryExists() {
        try {
            Path logDir = Paths.get("logs");
            if (!Files.exists(logDir)) {
                Files.createDirectories(logDir);
                logger.info("已创建日志目录: {}", logDir.toAbsolutePath());
            }
        } catch (IOException e) {
            logger.warn("创建日志目录失败", e);
        }
    }
} 