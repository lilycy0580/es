package com.es.demo.client;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.Objects;

@Component
public class EsClient {

    @Value("${elasticsearch.rest.hosts}")//读取es主机+端口配置
    private String hosts;

    @Value("${elasticsearch.rest.username}")//读取es用户名
    private String esUser;

    @Value("${elasticsearch.rest.password}")//读取es密码
    private String esPassword;


//    简单的客户端初始化
//    @Bean
//    public RestHighLevelClient initSimpleClient(){
//        //根据配置文件配置HttpHost数组
//        HttpHost[] httpHosts = Arrays.stream(hosts.split(",")).map(
//                host -> {
//                    //分隔es服务器IP和端口
//                    String[] hostParts = host.split(":");
//                    String hostName = hostParts[0];
//                    int port = Integer.parseInt(hostParts[1]);
//                    return new HttpHost(hostName, port, HttpHost.DEFAULT_SCHEME_NAME);
//                }).filter(Objects::nonNull).toArray(HttpHost[]::new);
//        return new RestHighLevelClient(RestClient.builder(httpHosts));//构建客户端
//    }

    // 生产环境下的安全验证功能    建立客户端与服务端连接时需要凭证用于鉴别用户身份,此凭证就是用户名和密码
    @Bean
    public RestHighLevelClient initClient1() {
        //根据配置文件配置HttpHost数组
        HttpHost[] httpHosts = Arrays.stream(hosts.split(",")).map(
                host -> {
                    //分隔es服务器IP和端口
                    String[] hostParts = host.split(":");
                    String hostName = hostParts[0];
                    int port = Integer.parseInt(hostParts[1]);
                    return new HttpHost(hostName, port, HttpHost.DEFAULT_SCHEME_NAME);
                }).filter(Objects::nonNull).toArray(HttpHost[]::new);
        //生成凭证
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(esUser, esPassword));//明文凭证
        //返回带验证的客户端
        return new RestHighLevelClient(
                RestClient.builder(
                        httpHosts)
                        .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                            public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                                httpClientBuilder.disableAuthCaching();
                                return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                            }
                        }));
    }
}


/*
CredentialsProvider 凭证类   调用其setCredentials() 设置凭证
UsernamePasswordCredentials 明文形式表示认证凭证  构建一个实例作为上面set的参数即可

RestClient.builder()构建一个RestClientBuilder实例后,调用RestClientBuilder.setHttpClientConfigCallback()完成认证 该set方法需要一个callback实例
 */
