package org.example;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Random;

public class GenerateTextFile {

    private static final List<String> OPERATORS = Arrays.asList("张三", "李四", "王五", "赵六", "孙七", "周八", "吴九", "郑十");
    private static final List<String> OPERATIONS = Arrays.asList(
        "测试服务器登录", "创建新用户", "删除旧数据", "更新系统配置", "备份数据库", "恢复备份数据", "检查系统日志", "重启服务器"
    );
    private static final Random random = new Random();

    public static void main(String[] args) {
        String filePath = "test.log"; // 输出文件路径
        int recordCount = 100000; // 生成 100,000 条记录

        try (BufferedWriter writer = new BufferedWriter(
                new OutputStreamWriter(new FileOutputStream(filePath), StandardCharsets.UTF_8))) {
            for (int i = 0; i < recordCount; i++) {
                // 示例数据
                long timestamp = new Date().getTime();
                String operator = getRandomOperator(); // 随机抽取操作者
                String operation = getRandomOperations(random.nextInt(200) + 1); // 随机抽取并组装操作内容

                // 转义字段中的特殊字符
                operator = escapeSpecialCharacters(operator);
                operation = escapeSpecialCharacters(operation);

                // 构建记录
                String record;
                if (random.nextDouble() < 0.1) {
                    // 10% 的概率生成不合法记录
                    int invalidType = random.nextInt(1000);
                    switch (invalidType) {
                        case 0:
                            // 缺少字段（只保留时间戳）
                            record = timestamp + ";";
                            break;
                        case 1:
                            // 多余字段（4个字段）
                            record = timestamp + "|" + operator + "|" + i + "|" + operation + ";";
                            break;
                        case 2:
                            // 包含未转义的特殊字符
                            record = timestamp + "|operator|invalid|field" + ";" ;
                            break;
                        case 3:
                            // 字段内容超长（字段2超限）
                            record = timestamp + "|超长字段内容:" + generateRandomString(40000) + "|" + i + ":" + operation + ";";
                            break;
                        case 4:
                            // 字段内容超长（字段3超限）
                            record = timestamp + "|" + operator + "|" + i + ":超长字段内容:" + generateRandomString(40000) + ";";
                            break;
                        default:
                            record = timestamp + "|" + operator + "|" + i + ":" + operation + ";";
                    }
                } else {
                    // 正常记录
                    record = timestamp + "|" + operator + "|" + i + ":" + operation + ";";
                }


                // 写入文件
                writer.write(record);
            }
            System.out.println("文件生成成功: " + filePath);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static String generateRandomString(int length) {
    StringBuilder sb = new StringBuilder(length);
    for (int i = 0; i < length; i++) {
        sb.append('x');
    }
    return sb.toString();
}


    // 转义字段中的特殊字符
    private static String escapeSpecialCharacters(String input) {
        return input.replace("|", "\\|").replace(";", "\\;");
    }

    // 随机抽取操作者
    private static String getRandomOperator() {
        return OPERATORS.get(random.nextInt(OPERATORS.size()));
    }

    // 随机抽取并组装操作内容
    private static String getRandomOperations(int count) {
//        Collections.shuffle(OPERATIONS); // 打乱操作内容列表
        StringBuilder sb = new StringBuilder();
        Random random = new Random();
        for (int i = 0; i < count ; i++) {
            if (sb.length() > 0) {
                sb.append(", "); // 用逗号分隔多个操作内容
            }
            sb.append(OPERATIONS.get(random.nextInt(OPERATIONS.size())));
        }
        return sb.toString();
    }
}
