package com.knowledgebase.qdrant.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

/**
 * 表示知识库中的文档
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Document {
    /**
     * 文档唯一标识符
     */
    private String id;
    
    /**
     * 文档标题
     */
    private String title;
    
    /**
     * 文档内容
     */
    private String content;
    
    /**
     * 文档来源文件路径
     */
    private String sourcePath;
    
    /**
     * 文档向量嵌入
     */
    private List<Float> embedding;
    
    /**
     * 最后修改时间
     */
    private LocalDateTime lastModified;
    
    /**
     * 文件哈希值，用于确定文件是否被修改
     */
    private String contentHash;
    
    /**
     * 根据文件路径创建文档
     */
    public static Document fromPath(Path path, String content, String contentHash) {
        // 使用UUID作为ID，这符合Qdrant的要求
        String id = UUID.randomUUID().toString();
        
        String title = path.getFileName().toString();
        
        Document doc = new Document();
        doc.setId(id);
        doc.setTitle(title);
        doc.setContent(content);
        doc.setSourcePath(path.toString());
        doc.setContentHash(contentHash);
        doc.setLastModified(LocalDateTime.now());
        
        return doc;
    }

    /**
     * 根据文件路径和指定ID创建文档（用于更新现有文档）
     */
    public static Document fromPathWithId(Path path, String content, String contentHash, String existingId) {
        String title = path.getFileName().toString();
        
        Document doc = new Document();
        doc.setId(existingId);
        doc.setTitle(title);
        doc.setContent(content);
        doc.setSourcePath(path.toString());
        doc.setContentHash(contentHash);
        doc.setLastModified(LocalDateTime.now());
        
        return doc;
    }
} 