package org.fordes.adg.rule;

import cn.hutool.core.date.DateUtil;
import cn.hutool.core.date.TimeInterval;
import cn.hutool.core.io.FileUtil;
import cn.hutool.core.map.MapUtil;
import cn.hutool.core.thread.ExecutorBuilder;
import cn.hutool.core.thread.ThreadUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.core.util.URLUtil;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadPoolExecutor;
import java.io.File; // 添加这一行
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.fordes.adg.rule.config.OutputConfig;
import org.fordes.adg.rule.config.RuleConfig;
import org.fordes.adg.rule.enums.RuleType;
import org.fordes.adg.rule.thread.LocalRuleThread;
import org.fordes.adg.rule.thread.RemoteRuleThread;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@AllArgsConstructor
@SpringBootApplication
public class AdgRuleApplication implements ApplicationRunner {

    private final RuleConfig ruleConfig;
    private final OutputConfig outputConfig;
    private final ThreadPoolExecutor executor = ExecutorBuilder.create()
            .setCorePoolSize(4) // 调整核心线程数
            .setMaxPoolSize(8) // 调整最大线程数
            .setHandler(new ThreadPoolExecutor.CallerRunsPolicy())
            .build();

    @Override
    public void run(ApplicationArguments args) throws Exception {
        TimeInterval interval = DateUtil.timer();

        // 初始化，根据配置建立文件
        final Map<RuleType, Set<File>> typeFileMap = MapUtil.newHashMap();
        if (!outputConfig.getFiles().isEmpty()) {
            outputConfig.getFiles().forEach((fileName, types) -> {
                File file = Util.createFile(outputConfig.getPath() + File.separator + fileName);

                // 添加标题行到文件
                try {
                    String titleLine = Constant.TITLE_TEMPLATE.replace("{}", fileName);
                    FileUtil.writeUtf8String(titleLine + "\n", file); // 写入标题行
                } catch (IOException e) {
                    log.error("Failed to write title line to {}: {}", fileName, e.getMessage());
                }

                types.forEach(type -> Util.safePut(typeFileMap, type, file));

                // 添加头部信息到文件
                try {
                    String header = Constant.REPO;
                    FileUtil.writeUtf8String(header + "\n", file, true); // 追加模式写入头部信息
                } catch (IOException e) {
                    log.error("Failed to write header to {}: {}", fileName, e.getMessage());
                }
            });
        }

        // 使用布隆过滤器实现去重
        BloomFilter<String> filter = BloomFilter.create(Funnels.stringFunnel(StandardCharsets.UTF_8), 1000000);

        // 远程规则
        ruleConfig.getRemote().stream()
                .filter(StrUtil::isNotBlank)
                .map(URLUtil::normalize)
                .forEach(e -> executor.execute(new RemoteRuleThread(e, typeFileMap, filter)));

        // 本地规则
        ruleConfig.getLocal().stream()
                .filter(StrUtil::isNotBlank)
                .map(e -> {
                    e = FileUtil.normalize(e);
                    if (FileUtil.isAbsolutePath(e)) {
                        return e;
                    }
                    return FileUtil.normalize(Constant.LOCAL_RULE_SUFFIX + File.separator + e);
                })
                .forEach(e -> executor.execute(new LocalRuleThread(e, typeFileMap, filter)));

        while (true) {
            if (executor.getActiveCount() > 0) {
                ThreadUtil.safeSleep(1000);
            } else {
                log.info("Done! {} ms", interval.intervalMs());
                System.exit(0);
            }
        }
    }

    public static void main(String[] args) {
        SpringApplication.run(AdgRuleApplication.class, args);
    }
}
