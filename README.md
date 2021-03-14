# tenet
### Kafka增强客户端
=================
### Build a jar and run it ###
```
mvn clean package
```
### 0.0.1.6-SNAPSHOT 功能介绍 ###
- 实现kafka consumer producer
  - consumer单线程poll消息，并同步批量写入日志库
  - porducer单线程异步发送消息，批量写入日志库（测试中）
- 业务处理处于使用线程池技术，多线程处理逻辑
  - 业务方依赖此jar包，实现AbstractKafkaTenetNode类的接口 
  - 异步业务逻辑支持 （开发中）
### 启动项目
```
需要引入springboot，无需任何注解@，项目通过autoconfig启动
maven需要配置所依赖的版本和内部私服
