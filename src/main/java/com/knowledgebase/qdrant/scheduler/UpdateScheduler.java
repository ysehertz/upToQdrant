package com.knowledgebase.qdrant.scheduler;

import com.knowledgebase.qdrant.config.AppConfig;
import com.knowledgebase.qdrant.service.QdrantService;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

/**
 * 更新调度器，负责定期执行知识库更新
 */
public class UpdateScheduler {
    private static final Logger logger = LoggerFactory.getLogger(UpdateScheduler.class);
    
    private final QdrantService qdrantService;
    private final AppConfig config;
    private Scheduler scheduler;
    
    public UpdateScheduler(QdrantService qdrantService, AppConfig config) {
        this.qdrantService = qdrantService;
        this.config = config;
    }
    
    /**
     * 启动调度器
     */
    public void start() {
        try {
            // 创建调度器
            SchedulerFactory schedulerFactory = new StdSchedulerFactory();
            scheduler = schedulerFactory.getScheduler();
            
            // 创建更新任务
            JobDetail job = JobBuilder.newJob(UpdateJob.class)
                    .withIdentity("updateJob", "group1")
                    .build();
            
            // 创建触发器，使用配置的 cron 表达式
            CronTrigger trigger = TriggerBuilder.newTrigger()
                    .withIdentity("updateTrigger", "group1")
                    .withSchedule(CronScheduleBuilder.cronSchedule(config.getUpdateCron()))
                    .build();
            
            // 创建服务上下文，包含qdrantService和配置信息，通过这种方式传递非序列化对象
            ServiceContext serviceContext = new ServiceContext(qdrantService);
            scheduler.getContext().put("serviceContext", serviceContext);
            
            // 将任务和触发器注册到调度器
            scheduler.scheduleJob(job, trigger);
            
            // 启动调度器
            scheduler.start();
            
            logger.info("调度器已启动，知识库将按计划更新: {}", config.getUpdateCron());
        } catch (SchedulerException e) {
            logger.error("启动调度器失败", e);
            throw new RuntimeException("无法启动更新调度器", e);
        }
    }
    
    /**
     * 停止调度器
     */
    public void stop() {
        try {
            if (scheduler != null && !scheduler.isShutdown()) {
                scheduler.shutdown();
                logger.info("调度器已停止");
            }
        } catch (SchedulerException e) {
            logger.error("停止调度器失败", e);
        }
    }
    
    /**
     * 服务上下文，保存了服务实例
     * 注意: Quartz会序列化Job实例，但不能序列化job数据中的复杂对象
     * 因此我们使用SchedulerContext来传递服务实例
     */
    public static class ServiceContext {
        private final QdrantService qdrantService;
        
        public ServiceContext(QdrantService qdrantService) {
            this.qdrantService = qdrantService;
        }
        
        public QdrantService getQdrantService() {
            return qdrantService;
        }
    }
    
    /**
     * 知识库更新任务
     */
    public static class UpdateJob implements Job {
        private static final Logger logger = LoggerFactory.getLogger(UpdateJob.class);
        
        @Override
        public void execute(JobExecutionContext context) throws JobExecutionException {
            try {
                logger.info("开始执行定期知识库更新任务");
                
                // 从SchedulerContext获取服务上下文
                SchedulerContext schedulerContext = context.getScheduler().getContext();
                ServiceContext serviceContext = (ServiceContext) schedulerContext.get("serviceContext");
                
                if (serviceContext == null) {
                    throw new JobExecutionException("无法获取服务上下文");
                }
                
                QdrantService qdrantService = serviceContext.getQdrantService();
                
                if (qdrantService == null) {
                    throw new JobExecutionException("找不到 QdrantService 实例");
                }
                
                // 执行知识库同步
                qdrantService.syncKnowledgeBase();
                
                logger.info("定期知识库更新任务完成");
            } catch (Exception e) {
                logger.error("执行知识库更新任务时出错", e);
                throw new JobExecutionException("知识库更新失败", e);
            }
        }
    }
} 