# 知识库上传工具

这是一个Java应用程序，用于将本地文本知识库上传到Qdrant Cloud向量数据库，并通过计划任务保持同步更新。

## 功能特点

1. 支持从本地目录读取TXT和MD格式的文本文件
2. 使用OpenAI的嵌入模型将文本转换为向量
3. 增量上传文件到Qdrant Cloud
4. 自动定期更新知识库（默认每10天一次）
5. 使用文件内容哈希检测文件变更
6. 支持并行处理多个文件
7. 完整的日志记录
8. 字段名适配Spring AI框架

## 数据结构

每个文档在Qdrant中存储的结构：
```json
{
  "id": "uuid格式的文档ID",
  "vector": [1536维的向量数组],
  "payload": {
    "title": "文件名",
    "doc_content": "文档内容（适配Spring AI）",
    "sourcePath": "源文件路径",
    "contentHash": "文件内容MD5哈希值",
    "lastModified": "最后修改时间"
  }
}
```

## 环境要求

- Java 17或更高版本
- Maven 3.6或更高版本
- Qdrant Cloud账号及API Key
- OpenAI API Key

## 配置说明

在`src/main/resources/application.properties`中配置以下参数：

```
# Qdrant Cloud配置
qdrant.url=https://your-instance.qdrant.tech
qdrant.api.key=your-api-key-here
qdrant.collection.name=knowledge_base
qdrant.vector.size=1536

# OpenAI配置
openai.api.key=your-openai-api-key
openai.embedding.model=text-embedding-ada-002

# 知识库配置
knowledge.base.directory=D:/knowledge_base
knowledge.base.extensions=txt,md

# 更新调度配置（每10天执行一次）
update.cron=0 0 0 */10 * ?

# 上传配置
upload.batch_size=100
upload.chunk_size=1000
```

## 构建与运行

### 构建项目

```bash
mvn clean package
```

这将在`target`目录下生成一个包含所有依赖的JAR文件。

### 运行项目

```bash
java -jar target/qdrant-uploader-1.0-SNAPSHOT-jar-with-dependencies.jar
```

## 使用方法

1. 修改配置文件中的参数，确保填入正确的API密钥和知识库目录
2. 运行应用程序
3. 应用程序会自动扫描指定目录下的文本文件，将其转换为向量并上传到Qdrant
4. 应用程序会根据配置的cron表达式定期检查并更新知识库

## 注意事项

- 首次运行会上传所有符合条件的文件
- 后续运行只会上传新增或修改过的文件
- 日志文件存储在`logs`目录下
- 更新过程中保证服务可用性
- 如果使用其他嵌入模型，请确保调整向量大小配置 