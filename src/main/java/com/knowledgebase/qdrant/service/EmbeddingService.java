package com.knowledgebase.qdrant.service;

import com.knowledgebase.qdrant.config.AppConfig;
import com.knowledgebase.qdrant.model.Document;
import com.theokanning.openai.OpenAiHttpException;
import com.theokanning.openai.embedding.EmbeddingRequest;
import com.theokanning.openai.embedding.EmbeddingResult;
import com.theokanning.openai.service.OpenAiService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * OpenAI嵌入服务，负责生成文本嵌入向量
 */
public class EmbeddingService {
    private static final Logger logger = LoggerFactory.getLogger(EmbeddingService.class);
    private static final int MAX_RETRY_ATTEMPTS = 3;
    private static final int RETRY_DELAY_MS = 1000;
    
    private final OpenAiService openAiService;
    private final String embeddingModel;
    
    public EmbeddingService(AppConfig config) {
        this.openAiService = new OpenAiService(config.getOpenAiApiKey(), Duration.ofSeconds(60));
        this.embeddingModel = config.getOpenAiEmbeddingModel();
        logger.info("初始化OpenAI嵌入服务，使用模型: {}", embeddingModel);
    }
    
    /**
     * 为文档生成嵌入向量
     */
    public Document generateEmbedding(Document document) {
        if (document.getContent() == null || document.getContent().trim().isEmpty()) {
            logger.warn("文档内容为空，无法生成嵌入向量: {}", document.getId());
            document.setEmbedding(Collections.emptyList());
            return document;
        }
        
        logger.debug("开始为文档生成嵌入向量: {} ({}字节)", document.getId(), document.getContent().length());
        
        // 截断超长的文本，以避免超出API限制
        String content = document.getContent();
        if (content.length() > 30000) {
            logger.warn("文档内容过长 ({} 字节)，将截断到30000字节", content.length());
            content = content.substring(0, 30000);
        }
        
        List<Float> embedding = createEmbedding(content);
        document.setEmbedding(embedding);
        
        logger.debug("文档嵌入向量生成完成: {}, 向量大小: {}", document.getId(), embedding.size());
        return document;
    }
    
    /**
     * 为文本创建嵌入向量
     */
    public List<Float> createEmbedding(String text) {
        int textLength = text.length();
        logger.debug("请求OpenAI生成嵌入向量, 文本长度: {} 字节", textLength);
        
        EmbeddingRequest request = EmbeddingRequest.builder()
                .model(embeddingModel)
                .input(Collections.singletonList(text))
                .build();
        
        long startTime = System.currentTimeMillis();
        
        for (int attempt = 0; attempt < MAX_RETRY_ATTEMPTS; attempt++) {
            try {
                logger.debug("调用OpenAI API尝试 {}/{}", attempt + 1, MAX_RETRY_ATTEMPTS);
                EmbeddingResult result = openAiService.createEmbeddings(request);
                
                long duration = System.currentTimeMillis() - startTime;
                logger.debug("获取OpenAI响应, 耗时: {}ms", duration);
                
                if (result.getData() != null && !result.getData().isEmpty()) {
                    List<Float> embeddings = result.getData().get(0).getEmbedding().stream()
                            .map(Double::floatValue)
                            .collect(Collectors.toList());
                    
                    logger.debug("嵌入向量生成成功，维度: {}", embeddings.size());
                    return embeddings;
                } else {
                    logger.error("OpenAI返回的嵌入结果为空");
                    return new ArrayList<>();
                }
            } catch (OpenAiHttpException e) {
                long duration = System.currentTimeMillis() - startTime;
                int statusCode = e.statusCode;
                String errorMessage = e.getMessage();
                
                logger.warn("OpenAI API调用失败，耗时: {}ms, 状态码: {}, 错误: {}", 
                        duration, statusCode, errorMessage);
                
                if (attempt < MAX_RETRY_ATTEMPTS - 1) {
                    int retryDelay = RETRY_DELAY_MS * (attempt + 1);
                    logger.warn("尝试重试 ({}/{}): 将在 {}ms 后重试", 
                            attempt + 1, MAX_RETRY_ATTEMPTS, retryDelay);
                    try {
                        TimeUnit.MILLISECONDS.sleep(retryDelay);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("线程中断", ie);
                    }
                } else {
                    logger.error("生成嵌入向量失败，达到最大重试次数 ({}), 最后错误: {}", 
                            MAX_RETRY_ATTEMPTS, errorMessage, e);
                    throw new RuntimeException("无法生成嵌入向量: " + errorMessage, e);
                }
            }
        }
        
        // 这里正常不会执行到，因为最后一次失败会抛出异常
        return new ArrayList<>();
    }
    
    /**
     * 批量为文档生成嵌入向量
     */
    public List<Document> generateEmbeddings(List<Document> documents) {
        int totalDocuments = documents.size();
        logger.info("开始批量生成嵌入向量，文档数量: {}", totalDocuments);
        
        int processedCount = 0;
        int successCount = 0;
        int errorCount = 0;
        
        for (Document document : documents) {
            try {
                processedCount++;
                generateEmbedding(document);
                
                if (document.getEmbedding() != null && !document.getEmbedding().isEmpty()) {
                    successCount++;
                } else {
                    errorCount++;
                }
                
                // 每处理10个文档记录一次进度
                if (processedCount % 10 == 0 || processedCount == totalDocuments) {
                    logger.info("嵌入向量生成进度: {}/{} ({}%), 成功: {}, 失败: {}", 
                            processedCount, totalDocuments, 
                            Math.round((float)processedCount/totalDocuments*100),
                            successCount, errorCount);
                }
            } catch (Exception e) {
                logger.error("为文档生成嵌入向量失败: {}, 原因: {}", document.getId(), e.getMessage());
                errorCount++;
            }
        }
        
        logger.info("批量嵌入向量生成完成. 总数: {}, 成功: {}, 失败: {}", 
                totalDocuments, successCount, errorCount);
        return documents;
    }
} 