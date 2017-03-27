package com.hna.es.util;

import org.elasticsearch.client.Client;

/**
 * Created by GH-GAN on 2016/8/11.
 */
public class ES {
    public static Client client = ESClient.getClient();
}
