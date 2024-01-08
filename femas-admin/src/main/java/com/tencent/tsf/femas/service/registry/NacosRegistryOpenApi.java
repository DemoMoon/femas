/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tencent.tsf.femas.service.registry;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.alibaba.nacos.shaded.io.grpc.internal.JsonUtil;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.tencent.tsf.femas.common.constant.FemasConstant;
import com.tencent.tsf.femas.common.entity.EndpointStatus;
import com.tencent.tsf.femas.common.entity.Service;
import com.tencent.tsf.femas.common.entity.ServiceInstance;
import com.tencent.tsf.femas.common.httpclient.HttpClientResponse;
import com.tencent.tsf.femas.common.serialize.JSONSerializer;
import com.tencent.tsf.femas.common.util.HttpResult;
import com.tencent.tsf.femas.common.util.StringUtils;
import com.tencent.tsf.femas.entity.namespace.Namespace;
import com.tencent.tsf.femas.entity.param.RegistryInstanceParam;
import com.tencent.tsf.femas.entity.registry.ClusterServer;
import com.tencent.tsf.femas.entity.registry.RegistryConfig;
import com.tencent.tsf.femas.entity.registry.RegistryPageService;
import com.tencent.tsf.femas.entity.registry.ServiceBriefInfo;
import com.tencent.tsf.femas.entity.registry.nacos.NacosInstance;
import com.tencent.tsf.femas.entity.registry.nacos.NacosServer;
import com.tencent.tsf.femas.entity.registry.nacos.NacosService;

import java.util.*;

import lombok.Data;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

/**
 * @Author leoziltong@tencent.com
 * @Date: 2021/4/29 21:59
 */
@Component
public class NacosRegistryOpenApi extends RegistryOpenApiAdaptor {

    /**
     * nacos 版本1.4.1
     * https://nacos.io/zh-cn/docs/open-api.html
     */
//    private final static String CLUSTER_LIST = "/nacos/v1/ns/operator/servers";
    private final static String CLUSTER_LIST = "/nacos/v2/core/cluster/node/list";
    private final static String FETCH_SERVICE_LIST = "/nacos/v1/ns/catalog/services";
    private final static String FETCH_SERVICE_INSTANCE_LIST = "/nacos/v1/ns/catalog/instances";
    private final static String NAMESPCE_URL = "/nacos/v1/console/namespaces";
    private final static String LOGIN_URL = "/nacos/v1/auth/login";

    public static void main(String[] args) {
        String str="{\"accessToken\":\"eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJuYWNvcyIsImV4cCI6MTYwNTYyOTE2Nn0.2TogGhhr11_vLEjqKko1HJHUJEmsPuCxkur-CfNojDo\",\"tokenTtl\":18000,\"globalAdmin\":true}";
        HttpResult<String> result=JSONSerializer.deserializeStr(HttpResult.class,str);
        System.out.println(result.getData());
        String str2="{\"code\":0,\"message\":\"success\",\"data\":[{\"ip\":\"10.128.164.35\",\"port\":8848,\"state\":\"UP\",\"extendInfo\":{\"lastRefreshTime\":1664521263623,\"raftMetaData\":{\"metaDataMap\":{\"naming_instance_metadata\":{\"leader\":\"10.128.164.35:7848\",\"raftGroupMember\":[\"10.128.164.35:7848\"],\"term\":12},\"naming_persistent_service_v2\":{\"leader\":\"10.128.164.35:7848\",\"raftGroupMember\":[\"10.128.164.35:7848\"],\"term\":12},\"naming_service_metadata\":{\"leader\":\"10.128.164.35:7848\",\"raftGroupMember\":[\"10.128.164.35:7848\"],\"term\":12}}},\"raftPort\":\"7848\",\"readyToUpgrade\":true,\"version\":\"2.1.0\"},\"address\":\"10.128.164.35:8848\",\"failAccessCnt\":0,\"abilities\":{\"remoteAbility\":{\"supportRemoteConnection\":true},\"configAbility\":{\"supportRemoteMetrics\":false},\"namingAbility\":{\"supportJraft\":true}}}]}";
        HttpResult<String> result2=JSONSerializer.deserializeStr(HttpResult.class,str);
        System.out.println(result.getData());
        System.out.println(JSONSerializer.deserializeStr2List(NacosServer.Server.class, result.getData()));
        String str1="[{\"ip\":\"10.128.164.35\",\"port\":8848,\"state\":\"UP\",\"extendInfo\":{\"lastRefreshTime\":1664521263623,\"raftMetaData\":{\"metaDataMap\":{\"naming_instance_metadata\":{\"leader\":\"10.128.164.35:7848\",\"raftGroupMember\":[\"10.128.164.35:7848\"],\"term\":12},\"naming_persistent_service_v2\":{\"leader\":\"10.128.164.35:7848\",\"raftGroupMember\":[\"10.128.164.35:7848\"],\"term\":12},\"naming_service_metadata\":{\"leader\":\"10.128.164.35:7848\",\"raftGroupMember\":[\"10.128.164.35:7848\"],\"term\":12}}},\"raftPort\":\"7848\",\"readyToUpgrade\":true,\"version\":\"2.1.0\"},\"address\":\"10.128.164.35:8848\",\"failAccessCnt\":0,\"abilities\":{\"remoteAbility\":{\"supportRemoteConnection\":true},\"configAbility\":{\"supportRemoteMetrics\":false},\"namingAbility\":{\"supportJraft\":true}}}]";
        System.out.println(JSONSerializer.deserializeStr2List(NacosServer.Server.class, str1));
    }

    private String getToken(RegistryConfig config) {
        String url = selectOne(config);
        Map<String, Object> queryMap = new HashMap<>();
        queryMap.put("username", config.getUsername());
        queryMap.put("password", config.getPassword());
        try {
            //{"accessToken":"eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJuYWNvcyIsImV4cCI6MTYwNTYyOTE2Nn0.2TogGhhr11_vLEjqKko1HJHUJEmsPuCxkur-CfNojDo","tokenTtl":18000,"globalAdmin":true}
            HttpResult<String> result = httpClient.post(url.concat(LOGIN_URL), null, queryMap, null);
            TokenRes tokenRes = JSONObject.parseObject(result.getData(),TokenRes.class);;
            assert tokenRes != null;
            return tokenRes.getAccessToken();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "";
    }

    @Data
    private static class TokenRes{
      private String accessToken;
      private Long tokenTtl;
      private Boolean globalAdmin;
    }


    @Override
    public List<ClusterServer> clusterServers(RegistryConfig config) {
        String url = selectOne(config);
        try {
            HttpResult<String> result = httpClient.get(url.concat(CLUSTER_LIST).concat("?accessToken=").concat(getToken(config)), null, null);
            NacosServer nacosServer = null;
            Map map = null;
            if (HttpStatus.SC_OK == NumberUtils.toInt(result.getCode())) {
                nacosServer = JSONSerializer.deserializeStr(NacosServer.class, result.getData());
            }
            if (nacosServer != null) {
                List<NacosServer.Server> serverList = JSONSerializer.deserializeStr2List(NacosServer.Server.class,JSONObject.toJSONString(nacosServer.getData()));
                List<ClusterServer> clusterServers = new ArrayList<>(serverList.size());
                serverList.stream().forEach(s -> {
                    ClusterServer server = new ClusterServer();
                    server.setServerAddr(s.getAddress());
                    server.setState(s.getState());
                    server.setLastRefreshTime(
                            Optional.ofNullable(s).map(s1 -> s1.getExtendInfo()).map(e -> e.getLastRefreshTime())
                                    .get());
                    Map<String, Object> nacosConf = Optional.ofNullable(s.getExtendInfo()).map(m -> m.getRaftMetaData())
                            .map(rm -> rm.getMetaDataMap()).map(md -> md.getNamingPersistentService())
                            .orElse(Collections.EMPTY_MAP);
                    server.setClusterRole("follow");
                    if (!CollectionUtils.isEmpty(nacosConf)) {
                        String leader = (String) nacosConf.get("leader");
                        String leaderIp = leader.split(":")[0];
                        String leaderPort = leader.split(":")[1];
                        if (leaderIp.equalsIgnoreCase(s.getIp())
                                && leaderPort.equalsIgnoreCase(s.getExtendInfo() != null ? s.getExtendInfo().getRaftPort() : "")) {
                            server.setClusterRole("leader");
                        }
                    }
                    clusterServers.add(server);
                });
                return clusterServers;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return Collections.emptyList();
    }

    @Override
    public void freshServiceMapCache(RegistryConfig config) {

    }

    @Override
    public ServerMetrics fetchServerMetrics(RegistryConfig config) {
        return null;
    }

    @Override
    public RegistryPageService fetchServices(RegistryConfig config, RegistryInstanceParam registryInstanceParam) {
        String url = selectOne(config);
        RegistryPageService registryPageService = new RegistryPageService();
        registryPageService.setPageNo(registryInstanceParam.getPageNo());
        registryPageService.setPageSize(registryInstanceParam.getPageSize());
        try {
            Map<String, Object> queryMap = new HashMap<>();
            queryMap.put("accessToken", getToken(config));
            queryMap.put("pageNo", registryInstanceParam.getPageNo());
            queryMap.put("pageSize", registryInstanceParam.getPageSize());
            queryMap.put("namespaceId", registryInstanceParam.getNamespaceId());
            HttpResult<String> result = httpClient.get(url.concat(FETCH_SERVICE_LIST), null, queryMap);
            NacosService service = null;
            if (HttpStatus.SC_OK == NumberUtils.toInt(result.getCode())) {
                service = JSONSerializer.deserializeStr(NacosService.class, result.getData());
            }
            if (service != null && !CollectionUtils.isEmpty(service.getServiceList())) {
                List<NacosService.Service> serviceList = service.getServiceList();
                List<ServiceBriefInfo> briefInfos = new ArrayList<>();
                serviceList.stream().forEach(s -> {
                    ServiceBriefInfo serviceBriefInfo = new ServiceBriefInfo();
                    serviceBriefInfo.setServiceName(s.getName());
                    serviceBriefInfo.setInstanceNum(s.getIpCount());
                    briefInfos.add(serviceBriefInfo);
                });
                registryPageService.setServiceBriefInfos(briefInfos);
                registryPageService.setCount(service.getCount());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return registryPageService;
    }

    @Override
    public RegistryPageService fetchNamespaceServices(RegistryConfig config, String namespaceId, int pageNo,
                                                      int pageSize) {
        return null;
    }

    @Override
    public List<ServiceInstance> fetchServiceInstances(RegistryConfig config,
                                                       RegistryInstanceParam registryInstanceParam) {
        String url = selectOne(config);
        try {
            Map<String, Object> queryMap = new HashMap<>();
            queryMap.put("accessToken", getToken(config));
            queryMap.put("serviceName", registryInstanceParam.getServiceName());
            queryMap.put("clusterName", "DEFAULT");
            queryMap.put("pageNo", 1);
            queryMap.put("pageSize", 100);
            queryMap.put("namespaceId", registryInstanceParam.getNamespaceId());
            HttpResult<String> result = httpClient.get(url.concat(FETCH_SERVICE_INSTANCE_LIST), null, queryMap);
            NacosInstance nacosInstance = null;
            if (HttpStatus.SC_OK == NumberUtils.toInt(result.getCode())) {
                nacosInstance = JSONSerializer.deserializeStr(NacosInstance.class, result.getData());
            }
            if (nacosInstance != null && !CollectionUtils.isEmpty(nacosInstance.getList())) {
                List<ServiceInstance> serviceInstances = new ArrayList<>(nacosInstance.getList().size());
                List<NacosInstance.Instance> instances = nacosInstance.getList();
                instances.stream().forEach(i -> {
                    ServiceInstance instance = new ServiceInstance();
                    instance.setAllMetadata(i.getMetadata());
                    instance.setHost(i.getIp());
                    instance.setPort(NumberUtils.toInt(i.getPort()));
                    String nacosInstanceId = i.getInstanceId();
                    //某些版本的nacos返回的字段不包含InstanceId，使用femas的InstanceId兜底
                    if (StringUtils.isNotBlank(nacosInstanceId)) {
                        instance.setId(nacosInstanceId);
                    } else {
                        String femasInstanceId = i.getMetadata().get(FemasConstant.FEMAS_META_INSTANCE_ID_KEY);
                        instance.setId(femasInstanceId);
                    }
                    instance.setStatus(i.isEnabled() && i.isHealthy() ? EndpointStatus.UP : EndpointStatus.DOWN);
                    instance.setLastUpdateTime(i.getLastBeat());
                    instance.setService(new Service(i.getClusterName(), i.getServiceName()));
                    serviceInstances.add(instance);
                });
                return serviceInstances;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return Collections.EMPTY_LIST;
    }

    @Override
    public boolean healthCheck(String url) {
        return true;
    }

    @Override
    public boolean createNamespace(RegistryConfig config, Namespace namespace) {
        String url = selectOne(config);
        try {
            Map<String, Object> queryMap = new HashMap<>();
            queryMap.put("customNamespaceId", namespace.getNamespaceId());
            queryMap.put("namespaceName", namespace.getName());
            queryMap.put("namespaceDesc", namespace.getDesc());
            HttpResult<String> result = httpClient.post(url.concat(NAMESPCE_URL).concat("?accessToken=").concat(getToken(config)), null, queryMap, null);
            return Boolean.TRUE.toString().equals(result.getData());
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public boolean modifyNamespace(RegistryConfig config, Namespace namespace) {
        String url = selectOne(config);
        try {
            Map<String, Object> queryMap = new HashMap<>();
            queryMap.put("namespace", namespace.getNamespaceId());
            queryMap.put("namespaceShowName", namespace.getName());
            queryMap.put("namespaceDesc", namespace.getDesc());
            HttpResult<String> result = httpClient.put(url.concat(NAMESPCE_URL).concat("?accessToken=").concat(getToken(config)), null, queryMap);
            return Boolean.TRUE.toString().equals(result.getData());
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public boolean deleteNamespace(RegistryConfig config, Namespace namespace) {
        String url = selectOne(config);
        try {
            Map<String, Object> queryMap = new HashMap<>();
            queryMap.put("namespaceId", namespace.getNamespaceId());
            HttpResult<String> result = httpClient.delete(url.concat(NAMESPCE_URL).concat("?accessToken=").concat(getToken(config)), null, queryMap);
            return Boolean.TRUE.toString().equals(result.getData());
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public List<Namespace> allNamespaces(RegistryConfig config) {
        List namespaceList = new ArrayList();
        String url = selectOne(config);
        try {
            HttpResult<String> result = httpClient.get(url.concat(NAMESPCE_URL).concat("?accessToken=").concat(getToken(config)), null, null);
            Map map = JSONSerializer.deserializeStr(Map.class, result.getData());
            List<Map> namespaceMapList = (List) map.get("data");
            for (Map nsp : namespaceMapList) {
                Namespace namespace = new Namespace();
                namespace.setNamespaceId((String) nsp.get("namespace"));
                namespace.setName((String) nsp.get("namespaceShowName"));
                namespace.setRegistryId(Arrays.asList(config.getRegistryId()));
                namespaceList.add(namespace);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return namespaceList;
    }

}
