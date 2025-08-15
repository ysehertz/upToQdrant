package com.knowledgebase.qdrant.service;

import com.knowledgebase.qdrant.config.AppConfig;
import com.knowledgebase.qdrant.model.Document;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.net.URLEncoder;
import java.util.AbstractMap;

/**
 * Qdrant服务，负责与Qdrant数据库交互
 * 使用HTTP API直接与Qdrant通信
 */
public class QdrantService {
    private static final Logger logger = LoggerFactory.getLogger(QdrantService.class);
    
    private final HttpClient httpClient;
    private final AppConfig config;
    private final EmbeddingService embeddingService;
    private final String collectionName;
    private final String baseUrl;
    private final ObjectMapper objectMapper;
    
    public QdrantService(AppConfig config) {
        this.config = config;
        this.collectionName = config.getQdrantCollectionName();
        this.baseUrl = config.getQdrantUrl();
        
        // 初始化HTTP客户端
        this.httpClient = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_2)
                .connectTimeout(Duration.ofSeconds(30))
                .build();
        
        // 初始化JSON处理器
        this.objectMapper = new ObjectMapper();
        
        // 初始化嵌入服务
        this.embeddingService = new EmbeddingService(config);
        
        logger.info("初始化Qdrant服务，连接到: {}, 集合: {}", config.getQdrantUrl(), collectionName);
    }
    
    /**
     * 确保Qdrant中存在所需的集合，不存在则创建
     */
    public void ensureCollectionExists() {
        try {
            // 检查集合是否存在
            String checkUrl = baseUrl + "/collections/" + collectionName;
            HttpRequest checkRequest = createGetRequest(checkUrl);
            
            HttpResponse<String> checkResponse = httpClient.send(checkRequest, HttpResponse.BodyHandlers.ofString());
            
            // 如果不存在（返回404），则创建集合
            if (checkResponse.statusCode() == 404) {
                logger.info("集合{}不存在，开始创建", collectionName);
                
                // 正确的API端点应该是 /collections
                String createUrl = baseUrl + "/collections/" + collectionName;
                
                // 创建集合配置
                ObjectNode requestBody = objectMapper.createObjectNode();
                // 不需要name参数，因为已经在URL中指定了集合名称
                
                ObjectNode vectorsConfig = requestBody.putObject("vectors");
                vectorsConfig.put("size", config.getQdrantVectorSize());
                vectorsConfig.put("distance", "Cosine");
                
                // 使用PUT而不是POST
                HttpRequest createRequest = createPutRequest(createUrl, requestBody.toString());
                HttpResponse<String> createResponse = httpClient.send(createRequest, HttpResponse.BodyHandlers.ofString());
                
                if (createResponse.statusCode() >= 200 && createResponse.statusCode() < 300) {
                    logger.info("成功创建集合: {}", collectionName);
                } else {
                    logger.error("创建集合失败: {}, 状态码: {}, 响应: {}", 
                        collectionName, createResponse.statusCode(), createResponse.body());
                    throw new RuntimeException("创建集合失败: " + createResponse.body());
                }
            } else if (checkResponse.statusCode() == 200) {
                logger.info("集合已存在: {}", collectionName);
            } else {
                logger.error("检查集合状态失败, 状态码: {}, 响应: {}", 
                    checkResponse.statusCode(), checkResponse.body());
                throw new RuntimeException("检查集合状态失败: " + checkResponse.body());
            }
        } catch (Exception e) {
            logger.error("检查或创建集合时出错", e);
            throw new RuntimeException("无法确保集合存在", e);
        }
    }
    
    /**
     * 同步知识库文件到Qdrant
     */
    public void syncKnowledgeBase() {
        Path knowledgeBasePath = config.getKnowledgeBaseDirectory();
        List<String> extensions = config.getKnowledgeBaseExtensions();
        
        logger.info("开始同步知识库: {}", knowledgeBasePath);
        logger.info("支持的文件格式: {}", String.join(", ", extensions));
        
        if (!Files.exists(knowledgeBasePath)) {
            logger.error("知识库目录不存在: {}", knowledgeBasePath);
            throw new RuntimeException("知识库目录不存在");
        }
        
        try {
            // 获取所有知识库文件
            List<Path> knowledgeFiles = findKnowledgeFiles(knowledgeBasePath, extensions);
            logger.info("发现{}个知识库文件", knowledgeFiles.size());
            
            if (knowledgeFiles.size() > 0) {
                logger.info("前5个文件示例: {}", 
                    knowledgeFiles.stream().limit(5).map(Path::toString).collect(Collectors.joining(", ")));
            }
            
            // 处理文件并上传到Qdrant
            processBatch(knowledgeFiles);
            
            logger.info("知识库同步完成，总共处理了{}个文件", knowledgeFiles.size());
        } catch (Exception e) {
            logger.error("同步知识库时出错: {}", e.getMessage(), e);
            throw new RuntimeException("同步知识库失败", e);
        }
    }
    
    /**
     * 查找知识库目录中的所有文件
     */
    private List<Path> findKnowledgeFiles(Path directory, List<String> extensions) throws IOException {
        try (Stream<Path> paths = Files.walk(directory)) {
            return paths
                    .filter(Files::isRegularFile)
                    .filter(p -> {
                        String ext = FilenameUtils.getExtension(p.toString()).toLowerCase();
                        return extensions.contains(ext);
                    })
                    .collect(Collectors.toList());
        }
    }
    
    /**
     * 处理文件批次
     */
    private void processBatch(List<Path> files) {
        int batchSize = config.getUploadBatchSize();
        int totalFiles = files.size();
        int processedFiles = 0;
        int skippedFiles = 0;
        int errorFiles = 0;
        int uploadedFiles = 0;
        int updatedFiles = 0;
        
        logger.info("开始处理文件批次，总文件数: {}, 批次大小: {}", totalFiles, batchSize);
        
        List<Document> batch = new ArrayList<>(batchSize);
        
        for (Path file : files) {
            try {
                logger.debug("正在处理文件: {}", file);
                String content = FileUtils.readFileToString(file.toFile(), StandardCharsets.UTF_8);
                String contentHash = DigestUtils.md5Hex(content);
                
                logger.debug("文件大小: {}字节, 哈希值: {}", content.length(), contentHash);
                
                // 检查文件是否已存在且未修改
                Map.Entry<Boolean, String> result = shouldProcessFile(file.toString(), contentHash);
                boolean needsProcessing = result.getKey();
                String existingId = result.getValue();
                
                if (!needsProcessing) {
                    logger.debug("文件未修改，跳过: {}", file);
                    skippedFiles++;
                    processedFiles++;
                    continue;
                }
                
                logger.debug("文件需要处理: {}", file);
                Document document;
                
                if (existingId != null && !existingId.isEmpty()) {
                    // 文件存在但需要更新，使用现有ID
                    document = Document.fromPathWithId(file, content, contentHash, existingId);
                    logger.debug("正在更新现有文档，ID: {}", existingId);
                    updatedFiles++;
                } else {
                    // 文件不存在，创建新文档
                    document = Document.fromPath(file, content, contentHash);
                    logger.debug("创建新文档，ID: {}", document.getId());
                }
                
                batch.add(document);
                
                // 当批次达到指定大小时进行处理
                if (batch.size() >= batchSize || processedFiles + batch.size() >= totalFiles) {
                    logger.info("批次已满或已处理完所有文件，开始上传批次，大小: {}", batch.size());
                    int batchProcessedCount = uploadBatch(batch);
                    uploadedFiles += batchProcessedCount;
                    logger.info("批次处理完成，成功上传: {}/{}", batchProcessedCount, batch.size());
                    batch.clear();
                }
            } catch (IOException e) {
                logger.warn("读取文件失败: {}, 原因: {}", file, e.getMessage(), e);
                errorFiles++;
            } catch (Exception e) {
                logger.warn("处理文件时出现意外错误: {}, 原因: {}", file, e.getMessage(), e);
                errorFiles++;
            }
            
            processedFiles++;
            if (processedFiles % 10 == 0) {
                logger.info("处理进度: {}/{} 个文件 ({}%), 已跳过: {}, 更新: {}, 错误: {}, 已上传: {}", 
                    processedFiles, totalFiles, 
                    Math.round((float)processedFiles/totalFiles*100), 
                    skippedFiles, updatedFiles, errorFiles, uploadedFiles);
            }
        }
        
        // 处理最后一批文件
        if (!batch.isEmpty()) {
            logger.info("处理最后一批文件，大小: {}", batch.size());
            int batchProcessedCount = uploadBatch(batch);
            uploadedFiles += batchProcessedCount;
            logger.info("最后批次处理完成，成功上传: {}/{}", batchProcessedCount, batch.size());
        }
        
        logger.info("所有文件处理完成. 总结: 总计: {}, 已跳过: {}, 更新: {}, 错误: {}, 已上传: {}", 
            totalFiles, skippedFiles, updatedFiles, errorFiles, uploadedFiles);
    }
    
    /**
     * 判断文件是否需要处理
     * @param filepath 文件路径
     * @param contentHash 文件内容的哈希值
     * @return 包含两个元素的Map.Entry：第一个元素(Key)表示是否需要处理，第二个元素(Value)是现有文档ID（如果存在）
     */
    private Map.Entry<Boolean, String> shouldProcessFile(String filepath, String contentHash) {
        try {
            logger.debug("检查文件是否需要处理: {}", filepath);
            
            // 构建搜索查询
            String searchUrl = baseUrl + "/collections/" + collectionName + "/points/scroll";
            
            // 构建正确的过滤器格式
            ObjectNode requestBody = objectMapper.createObjectNode();
            
            // 构建过滤器
            ObjectNode filter = objectMapper.createObjectNode();
            
            // 创建正确格式的条件
            ObjectNode condition = objectMapper.createObjectNode();
            condition.put("key", "sourcePath");
            
                   // 匹配条件
            ObjectNode matchValue = objectMapper.createObjectNode();
            matchValue.put("value", filepath);
            condition.set("match", matchValue);
            
            // 将条件加入到must数组
            ArrayNode must = objectMapper.createArrayNode();
            must.add(condition);
            
            // 将must条件设置到filter
            ObjectNode filterObj = objectMapper.createObjectNode();
            filterObj.set("must", must);
            
            // 设置filter和limit
            requestBody.set("filter", filterObj);
            requestBody.put("limit", 1);
            
            String requestBodyStr = requestBody.toString();
            logger.debug("搜索Qdrant中的文件记录: {}", requestBodyStr);
            
            HttpRequest searchRequest = createPostRequest(searchUrl, requestBodyStr);
            HttpResponse<String> searchResponse = httpClient.send(searchRequest, HttpResponse.BodyHandlers.ofString());
            
            if (searchResponse.statusCode() == 200) {
                // 解析响应
                ObjectNode responseJson = (ObjectNode) objectMapper.readTree(searchResponse.body());
                ArrayNode points = (ArrayNode) responseJson.get("result").get("points");
                
                if (points.size() > 0) {
                    // 文件已存在，检查哈希值是否相同
                    ObjectNode point = (ObjectNode) points.get(0);
                    ObjectNode payload = (ObjectNode) point.get("payload");
                    String storedHash = payload.get("contentHash").asText();
                    
                    // 获取文档ID，用于更新
                    String documentId = point.get("id").asText();
                    
                    boolean needsUpdate = !contentHash.equals(storedHash);
                    logger.debug("文件在数据库中已存在. ID: {}, 存储的哈希值: {}, 当前哈希值: {}, 需要更新: {}", 
                        documentId, storedHash, contentHash, needsUpdate);
                    
                    // 返回是否需要更新以及文档ID
                    return new AbstractMap.SimpleEntry<>(needsUpdate, documentId);
                } else {
                    logger.debug("文件在数据库中不存在，需要处理");
                    return new AbstractMap.SimpleEntry<>(true, "");
                }
            } else {
                logger.warn("搜索Qdrant中的文件记录失败，状态码: {}, 响应: {}", 
                    searchResponse.statusCode(), searchResponse.body());
                return new AbstractMap.SimpleEntry<>(true, ""); // 出错时保守处理，尝试处理文件
            }
        } catch (Exception e) {
            logger.error("检查文件状态时出错: {}, 原因: {}", filepath, e.getMessage(), e);
            // 出错时保守处理，尝试处理文件
            return new AbstractMap.SimpleEntry<>(true, "");
        }
    }
    
    /**
     * 验证文档是否已成功上传
     * @param documentId 文档ID
     * @return 如果文档存在，返回true
     */
    private boolean verifyDocumentExists(String documentId) {
        try {
            logger.debug("验证文档是否存在: {}", documentId);
            
            // 构建API端点 - 确保URI安全
            String encodedId = URLEncoder.encode(documentId, StandardCharsets.UTF_8.toString());
            String getUrl = baseUrl + "/collections/" + collectionName + "/points/" + encodedId;
            
            HttpRequest getRequest = createGetRequest(getUrl);
            
            HttpResponse<String> getResponse = httpClient.send(getRequest, HttpResponse.BodyHandlers.ofString());
            
            if (getResponse.statusCode() == 200) {
                logger.debug("文档存在: {}", documentId);
                return true;
            } else {
                logger.warn("文档不存在或获取失败: {}, 状态码: {}, 响应: {}", 
                    documentId, getResponse.statusCode(), getResponse.body());
                return false;
            }
        } catch (Exception e) {
            logger.error("验证文档存在时出错: {}, 原因: {}", documentId, e.getMessage());
            return false;
        }
    }
    
    /**
     * 上传文档批次到Qdrant，完成后验证一个样本文档
     * @return 成功上传的文档数量
     */
    private int uploadBatch(List<Document> documents) {
        if (documents.isEmpty()) {
            logger.debug("批次为空，无需上传");
            return 0;
        }
        
        try {
            // 生成嵌入
            logger.info("开始为{}个文档生成嵌入向量", documents.size());
            documents = embeddingService.generateEmbeddings(documents);
            
            // 过滤掉没有嵌入的文档
            List<Document> validDocuments = documents.stream()
                    .filter(doc -> doc.getEmbedding() != null && !doc.getEmbedding().isEmpty())
                    .collect(Collectors.toList());
            
            if (validDocuments.isEmpty()) {
                logger.warn("没有有效的文档可以上传，所有文档都缺少嵌入向量");
                return 0;
            }
            
            if (validDocuments.size() < documents.size()) {
                logger.warn("部分文档无法生成嵌入向量，原始数量: {}, 有效数量: {}", 
                    documents.size(), validDocuments.size());
            }
            
            // 批量上传API URL
            String upsertUrl = baseUrl + "/collections/" + collectionName + "/points";
            
            // 准备请求体
            ObjectNode requestBody = objectMapper.createObjectNode();
            ArrayNode batchPoints = requestBody.putArray("points");
            
            // 记录ID，用于后续验证
            List<String> documentIds = new ArrayList<>();
            
            // 将文档转换为Qdrant点位
            for (Document doc : validDocuments) {
                ObjectNode point = objectMapper.createObjectNode();
                
                // 创建ID节点 - Qdrant要求使用特定格式
                // UUID格式需要放在字符串字段中
                point.put("id", doc.getId());
                documentIds.add(doc.getId());
                
                // 添加向量
                ArrayNode vector = point.putArray("vector");
                for (Float value : doc.getEmbedding()) {
                    vector.add(value);
                }
                
                // 添加元数据 - 字段名适配Spring AI
                ObjectNode payload = point.putObject("payload");
                payload.put("title", doc.getTitle());
                payload.put("doc_content", doc.getContent());  // 修改字段名为doc_content以适配Spring AI
                payload.put("sourcePath", doc.getSourcePath());
                payload.put("contentHash", doc.getContentHash());
                payload.put("lastModified", doc.getLastModified().toString());
                
                batchPoints.add(point);
                logger.debug("已准备文档: {} (ID: {})", doc.getTitle(), doc.getId());
            }
            
            // 发送请求前记录
            String requestBodyStr = requestBody.toString();
            logger.debug("上传Qdrant请求体(示例): {}", 
                requestBodyStr.length() > 1000 ? requestBodyStr.substring(0, 1000) + "..." : requestBodyStr);
            
            // 发送请求
            logger.info("开始上传{}个文档到Qdrant", validDocuments.size());
            HttpRequest upsertRequest = createPutRequest(upsertUrl, requestBodyStr);
            HttpResponse<String> upsertResponse = httpClient.send(upsertRequest, HttpResponse.BodyHandlers.ofString());
            
            if (upsertResponse.statusCode() == 200) {
                logger.info("成功上传 {} 个文档到Qdrant", validDocuments.size());
                
                // 验证上传成功 - 随机抽查一个文档
                if (!documentIds.isEmpty()) {
                    int randomIndex = (int) (Math.random() * documentIds.size());
                    String sampleId = documentIds.get(randomIndex);
                    boolean verificationResult = verifyDocumentExists(sampleId);
                    logger.info("上传验证{}：随机检查文档 {}", 
                        verificationResult ? "成功" : "失败", sampleId);
                }
                
                return validDocuments.size();
            } else {
                logger.error("上传文档失败，状态码: {}, 响应: {}", upsertResponse.statusCode(), upsertResponse.body());
                // 打印请求体的前100个字符，帮助调试
                logger.error("请求体预览（前100个字符）: {}", 
                    requestBodyStr.length() > 100 ? requestBodyStr.substring(0, 100) + "..." : requestBodyStr);
                throw new RuntimeException("上传文档失败: " + upsertResponse.body());
            }
        } catch (Exception e) {
            logger.error("上传文档批次时出错: {}", e.getMessage(), e);
            throw new RuntimeException("上传文档批次失败", e);
        }
    }
    
    /**
     * 创建GET请求
     */
    private HttpRequest createGetRequest(String url) {
        return HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Content-Type", "application/json")
                .header("api-key", config.getQdrantApiKey())
                .GET()
                .build();
    }
    
    /**
     * 创建POST请求
     */
    private HttpRequest createPostRequest(String url, String body) {
        return HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Content-Type", "application/json")
                .header("api-key", config.getQdrantApiKey())
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
    }
    
    /**
     * 创建PUT请求
     */
    private HttpRequest createPutRequest(String url, String body) {
        return HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Content-Type", "application/json")
                .header("api-key", config.getQdrantApiKey())
                .PUT(HttpRequest.BodyPublishers.ofString(body))
                .build();
    }
    
    /**
     * 关闭服务
     */
    public void close() {
        // HTTP客户端不需要显式关闭
        logger.info("Qdrant服务已关闭");
    }
} 