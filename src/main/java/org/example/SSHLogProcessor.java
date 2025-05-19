package org.example;
import com.jcraft.jsch.*;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CoderResult;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * SSHLogProcessor类用于通过SSH连接到远程服务器，读取日志文件，并处理日志内容。
 * 日志内容会被解析并写入CSV文件，无效的记录会被写入单独的文本文件。
 */
public class SSHLogProcessor {
    private static final int BUFFER_SIZE = 1013; // 固定缓冲区大小
    private static final int MAX_BUFFER_SIZE = 100 * 1024 * 1024; // 缓冲区最大限制（10MB）
    private static final int IO_BUFFER_SIZE = 30 * 1024 * 1024; // IO缓冲字符串最大限制（10MB）
    private static final int THREAD_POOL_SIZE = 1; // 线程池大小
    private static final int MAX_ITERATIONS = 10000; // 最大执行次数
    private static final int PRINT_MEMORY_INTERVAL = 1000; // 打印内存间隔
    private static final int MAX_RETRY_COUNT = 3; // 最大重试次数


    /**
     * 主函数，程序的入口。
     *
     * @param args 命令行参数，依次为远程IP地址、端口号、用户名和密码。
     */
    public static void main(String[] args) {
        if (args.length != 4) {
            System.out.println("Usage: java SSHLogProcessor <remoteIP> <port> <username> <password>");
            return;
        }

        String remoteIP = args[0];
        int port = Integer.parseInt(args[1]);
        String username = args[2];
        String password = args[3];

        ExecutorService executor = Executors.newFixedThreadPool(THREAD_POOL_SIZE);

        //打印缓冲区和IO缓冲字符串大小
        System.out.println("max Buffer size: " + MAX_BUFFER_SIZE/1024/1024 + " MB");
        System.out.println("IO Buffer size: " + IO_BUFFER_SIZE/1024/1024 + " MB");
        // 总耗时
        long duration = 0;
        List<Long> executionTimes = new ArrayList<>(); // 存储每次迭代的耗时

        // 循环执行日志处理任务，直到达到最大执行次数
        for (int i = 0; i < MAX_ITERATIONS; i++) {
            try {
                long startTime = System.currentTimeMillis();
                processLog(remoteIP, port, username, password, executor);
                long endTime = System.currentTimeMillis();
                long iterationTime = endTime - startTime;
                executionTimes.add(iterationTime); // 记录每次迭代的耗时
                System.out.println("Iteration " + (i + 1) + ": Execution time = " + iterationTime + " ms");
                if ((i + 1) % PRINT_MEMORY_INTERVAL == 0) {
                    printMemoryUsage();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        // 去掉一个最高耗时和一个最低耗时，计算平均耗时
        if (executionTimes.size() >= 3) { // 至少需要3次迭代才能去掉最高和最低
            executionTimes.sort(Long::compareTo); // 对耗时进行排序
            executionTimes.remove(0); // 去掉最低耗时
            executionTimes.remove(executionTimes.size() - 1); // 去掉最高耗时
            long totalTime = executionTimes.stream().mapToLong(Long::longValue).sum(); // 计算剩余耗时的总和
            long averageTime = totalTime / executionTimes.size(); // 计算平均耗时
            System.out.println("Average execution time (去掉最高和最低耗时): " + averageTime + " ms");
        } else {
            System.out.println("迭代次数不足，无法去掉最高和最低耗时");
        }

        executor.shutdown();
    }

    /**
     * 通过SSH连接到远程服务器，读取日志文件并处理日志内容。
     *
     * @param remoteIP 远程服务器的IP地址
     * @param port 远程服务器的端口号
     * @param username SSH连接的用户名
     * @param password SSH连接的密码
     * @param executor 用于异步处理日志内容的线程池
     * @throws JSchException 如果SSH连接失败
     * @throws IOException 如果读取日志文件失败
     */
    private static void processLog(String remoteIP, int port, String username, String password, ExecutorService executor)
            throws JSchException, IOException {
        int retryCount = 0;
        while (retryCount < MAX_RETRY_COUNT) {
            try {
                JSch jsch = new JSch();
                Session session = jsch.getSession(username, remoteIP, port);
                session.setPassword(password);
                session.setConfig("StrictHostKeyChecking", "no");
                session.connect();

                ChannelExec channel = (ChannelExec) session.openChannel("exec");
                channel.setCommand("cat test.log");
                channel.setInputStream(null);
                channel.setErrStream(System.err);

                InputStream in = channel.getInputStream();
                channel.connect();

                processLogContent(in, executor);

                channel.disconnect();
                session.disconnect();
                break; // 成功执行后退出循环
            } catch (JSchException | IOException e) {
                retryCount++;
                if (retryCount >= MAX_RETRY_COUNT) {
                    System.out.println("重试失败，抛出异常");
                    throw e; // 达到最大重试次数后抛出异常
                }else {
                    System.out.println("连接失败，重试中... (" + retryCount + "/" + MAX_RETRY_COUNT + ")");
                }
            }
        }
    }


    /**
     * 处理日志内容，将有效记录写入CSV文件，无效记录写入文本文件。
     *
     * @param logStream
     * @param executor
     */
    private static void processLogContent(InputStream logStream, ExecutorService executor) {
        StringBuilder recordBuilder = new StringBuilder();
        byte[] buffer = new byte[BUFFER_SIZE];
        int bytesRead;

        // 使用 CharsetDecoder 处理字节流
        CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder();
        //  创建 ByteBuffer 和 CharBuffer，字节容量为buffer的长度+3，+3是因为UTF-8编码最多有4个字节，不完整的UTF-8字符最多占3个字节，避免字符截断导致的乱码问题
        ByteBuffer byteBuffer = ByteBuffer.allocate(BUFFER_SIZE+3);
        CharBuffer charBuffer = CharBuffer.allocate(BUFFER_SIZE+3);

        try (BufferedWriter csvWriter = new BufferedWriter(new FileWriter("test.csv"));
             BufferedWriter invalidWriter = new BufferedWriter(new FileWriter("invalid_records.txt"))) {

            Future<Void> future = null;
            while ((bytesRead = logStream.read(buffer)) != -1) {
                // 将字节数据放入 ByteBuffer
                byteBuffer.put(buffer, 0, bytesRead);
                byteBuffer.flip(); // 切换到读模式

                // 解码字节为字符
                CoderResult result = decoder.decode(byteBuffer, charBuffer, false);
                if (result.isError()) {
                    throw new IOException("字符解码失败");
                }

                // 将字符数据追加到 StringBuilder
                charBuffer.flip(); // 切换到读模式
                recordBuilder.append(charBuffer);
                charBuffer.clear(); // 清空 CharBuffer

                // 检查缓冲区长度是否超过最大限制
                if (recordBuilder.length() > MAX_BUFFER_SIZE) {
                    String pattern = "(?<!\\\\);";
                    Pattern compiledPattern = Pattern.compile(pattern);
                    Matcher matcher = compiledPattern.matcher(recordBuilder);

                    if (matcher.find()) {
                        // 按记录分隔符分割日志内容
                        String[] records = recordBuilder.toString().split("(?<!\\\\);");
                        // 保留未完整处理的记录
                        recordBuilder = new StringBuilder(records[records.length - 1] + (recordBuilder.charAt(recordBuilder.length()-1) == ';' ? ";" : ""));

                        // 等待消费者线程处理完缓冲区数据
                        if (future != null) {
                            future.get();
                        }

                        future = (Future<Void>) executor.submit(() -> processRecords(records, records.length-1,csvWriter, invalidWriter));
//                        processRecords(records, records.length-1,csvWriter, invalidWriter);
                    } else {
                        throw new IOException("Buffer size exceeded without record separator.");
                    }
                }

                byteBuffer.compact(); // 切换到写模式，保留未处理的字节
            }

            // 处理最后一条记录
            if (recordBuilder.length() > 0) {
                StringBuilder finalRecordBuilder = recordBuilder;
                // 按记录分隔符分割日志内容
                String[] records = recordBuilder.toString().split("(?<!\\\\);");
//                future = (Future<Void>) executor.submit(() -> processRecords(records, records.length, csvWriter, invalidWriter));
//                future.get();
                processRecords(records, records.length, csvWriter, invalidWriter);
            }

//            System.out.println("recordCount:"+recordCount);
//            System.out.println("size:"+size);

        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        } catch (Exception e){
            throw e;
        }
    }


    private static void processRecords(String[] records, int size,BufferedWriter csvWriter, BufferedWriter invalidWriter) {
        StringBuilder recordBuilder = new StringBuilder();
        StringBuilder invalidBuilder = new StringBuilder();
        for (int i = 0; i < records.length && i < size; i++) {
            String[] fields = records[i].split("(?<!\\\\)\\|", -1);
            if (fields.length == 3 &&
                    fields[0] != null && fields[0].length() <= 32767 &&
                    fields[1] != null && fields[1].length() <= 32767) {
                String timestamp = formatTimestamp(fields[0]);
                String operation = fields[2];
                recordBuilder.append(timestamp + ",\"" + operation + "\"\n");
            } else {
                // 无效记录：写入无效记录文件
                String invalidReason = getInvalidReason(fields); // 获取无效原因
                invalidBuilder.append("原始记录: " + records[i] + "\n");
                invalidBuilder.append("无效原因: " + invalidReason + "\n");
                invalidBuilder.append("------------------\n"); // 分隔符
            }

            // 批量写入
            if (recordBuilder.length() >= IO_BUFFER_SIZE) {
                writeBatch(csvWriter, recordBuilder);

            }
            if (invalidBuilder.length() >= IO_BUFFER_SIZE) {
                writeBatch(invalidWriter, invalidBuilder);
            }
        }
        // 批量写入剩余数据
        if(recordBuilder.length() > 0) {
            writeBatch(csvWriter, recordBuilder);
        }
        if (invalidBuilder.length() > 0) {
            writeBatch(invalidWriter, invalidBuilder);
        }
    }

    /**
     * 获取无效记录的原因。
     *
     * @param fields 记录字段
     * @return 无效原因
     */
    private static String getInvalidReason(String[] fields) {
        if (fields.length != 3) {
            return "字段数量不符，应为 3 个字段，实际为 " + fields.length + " 个字段";
        }
        if (fields[0] == null || fields[0].length() > 32767) {
            return "字段 1（时间戳）无效：为空或长度超过 32767 字符";
        }
        if (fields[1] == null || fields[1].length() > 32767) {
            return "字段 2（操作）无效：为空或长度超过 32767 字符";
        }
        return "未知原因";
    }

    private static void writeBatch(BufferedWriter writer, StringBuilder stringBuilder) {
        try  {
            writer.write(stringBuilder.toString());
            stringBuilder.setLength(0);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    private static void processRecord(String record, BufferedWriter csvWriter, BufferedWriter invalidWriter) {
        // 按字段分隔符分割记录
        String[] fields = record.split("(?<!\\\\)\\|",-1);

        // 判断是否为有效记录
        synchronized (csvWriter) {
            try {
                //excel单元格长度最大为32767个字符
                if (fields.length == 3 &&
                        fields[0] != null && fields[0].length() <= 32767 &&
                        fields[1] != null && fields[1].length() <= 32767
                ) {
                    String timestamp = formatTimestamp(fields[0]);
                    String operation = fields[2];
                    csvWriter.write(timestamp + ",\"" + operation + "\"\n");
                } else {
                    invalidWriter.write(record + "\n");
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    /**
     * 将时间戳格式化为可读的日期时间字符串。
     *
     * @param timestamp 时间戳字符串
     * @return 格式化后的日期时间字符串，如果时间戳无效则返回"Invalid Timestamp"
     */
    private static String formatTimestamp(String timestamp) {
        try {
            long timeMillis = Long.parseLong(timestamp);
            Date date = new Date(timeMillis);
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            return sdf.format(date);
        } catch (NumberFormatException e) {
            return "Invalid Timestamp";
        }
    }

    /**
     * 打印当前JVM的内存使用情况和剩余可用内存。
     */
    private static void printMemoryUsage() {
        Runtime runtime = Runtime.getRuntime();
        long maxMemory = runtime.maxMemory(); // JVM最大可用内存
        long usedMemory = runtime.totalMemory() - runtime.freeMemory(); // 已使用内存
        long freeMemory = maxMemory - usedMemory; // 剩余可用内存

//        System.out.println("最大内存: " + maxMemory / (1024 * 1024) + " MB");
//        System.out.println("已使用内存: " + usedMemory / (1024 * 1024) + " MB");
        System.out.println("剩余可用内存: " + freeMemory / (1024 * 1024) + " MB");
    }

}
