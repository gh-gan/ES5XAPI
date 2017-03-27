package com.hna.es.util;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.InputStream;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Created by GH-GAN on 2016/8/8.
 */
public class ESClient {
    private static List<Client> clients = new ArrayList<Client>();

    private ESClient() {
        clients.add(createClient());
    }

    private static Client createClient() {
        Settings settings = Settings.builder()
                .put("client.transport.sniff", true)
                .put("cluster.name", ESConfig.clusterName).build();

        Client client = null;
        try {
            TransportClient transportClient = new PreBuiltTransportClient(settings);
            assert ESConfig.nodes != null;
            String[] hosts = ESConfig.nodes.split(",");
            for (String host : hosts) {
                transportClient.addTransportAddress(
                        new InetSocketTransportAddress(InetAddress.getByName(host), Integer.parseInt(ESConfig.transportPort)));
            }
            client = transportClient;
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        return client;
    }

    public synchronized static Client getClient() {
        if (clients.size() > 0) return clients.remove(0);
        else return createClient();
    }

    public static void closeClient(Client client) {
        clients.add(client);
    }
}
