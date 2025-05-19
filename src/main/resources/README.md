# SSHLogProcessor

SSHLogProcessor 是一个通过 SSH 连接到远程服务器，读取日志文件并处理日志内容的 Java 程序。日志内容会被解析并写入 CSV 文件，无效的记录会被写入单独的文本文件。

## 功能

- **SSH 连接**：通过 SSH 连接到远程服务器，读取日志文件。
- **日志处理**：解析日志内容，将有效记录写入 CSV 文件，无效记录写入文本文件。
- **批量写入**：使用批量写入机制，减少 I/O 操作次数，提升性能。
- **重试机制**：在网络瞬断时，支持重试连接和日志读取。
- **内存监控**：实时监控 JVM 内存使用情况，避免内存溢出。

## 使用方法

### 1. 编译项目

确保已安装 JDK，然后使用以下命令编译项目：


bash 
```
javac -d . src/main/java/org/example/SSHLogProcessor.java
```

### 2. 运行程序

运行程序时，需要提供以下命令行参数：

bash
```
java org.example.SSHLogProcessor <remoteIP> <port> <username> <password>
```

#### 参数说明

- `<remoteIP>`：远程服务器的 IP 地址。
- `<port>`：远程服务器的 SSH 端口号。
- `<username>`：SSH 连接的用户名。
- `<password>`：SSH 连接的密码。

### 输入文件test.log说明：
utf-8编码，字段分隔符为竖线，记录分隔符为分号。字段内容中的字段分隔符和记录分隔符，前面加有转义符”\”。字段说明如下：
- 字段1：时间戳，整数。1970年1月1日到当前的毫秒数。如 1595830781357
- 字段2：操作者，字符串。如 张三
- 字段3：操作内容，字符串。如 测试服务器登录



### 3. 输出文件

#### 输出文件test.csv格式说明：
- 字段分隔符为逗号，记录分隔符为换行符。
- 字段1：时间戳，日期类型。如 2020-07-28 10:35:10
- 字段2：操作内容，字符串。如 登录服务器

程序会生成以下文件：

- [test.csv](file://D:\Git\telecomExam\test.csv)：包含有效记录的 CSV 文件。
- [invalid_records.txt](file://D:\Git\telecomExam\invalid_records.txt)：包含无效记录的文本文件。

## 配置参数

程序支持以下配置参数（在代码中修改）：

| 参数名                  | 描述                         | 默认值        |
|------------------------|------------------------------|------------|
| [BUFFER_SIZE](file://D:\Git\telecomExam\src\main\java\org\example\SSHLogProcessor.java#L21-L21)          | 固定缓冲区大小                 | `1013` 字节  |
| [MAX_BUFFER_SIZE](file://D:\Git\telecomExam\src\main\java\org\example\SSHLogProcessor.java#L22-L22)      | 缓冲区最大限制                 | `100 MB`   |
| [IO_BUFFER_SIZE](file://D:\Git\telecomExam\src\main\java\org\example\SSHLogProcessor.java#L23-L23)       | IO 缓冲字符串最大限制           | `30 MB`    |
| [THREAD_POOL_SIZE](file://D:\Git\telecomExam\src\main\java\org\example\SSHLogProcessor.java#L24-L24)     | 线程池大小                     | `1`        |
| [MAX_ITERATIONS](file://D:\Git\telecomExam\src\main\java\org\example\SSHLogProcessor.java#L25-L25)       | 最大执行次数                   | `10000`    |
| [PRINT_MEMORY_INTERVAL](file://D:\Git\telecomExam\src\main\java\org\example\SSHLogProcessor.java#L26-L26)| 打印内存间隔                   | `1000` 次迭代 |
| [MAX_RETRY_COUNT](file://D:\Git\telecomExam\src\main\java\org\example\SSHLogProcessor.java#L27-L27)      | 最大重试次数                   | `3` 次      |

## 示例

bash 
``` 
java org.example.SSHLogProcessor 192.168.1.100 22 user password 
```
