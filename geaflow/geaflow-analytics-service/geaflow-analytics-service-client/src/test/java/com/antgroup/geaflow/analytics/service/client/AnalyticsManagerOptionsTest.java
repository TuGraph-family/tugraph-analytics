package com.antgroup.geaflow.analytics.service.client;

import org.testng.Assert;
import org.testng.annotations.Test;

public class AnalyticsManagerOptionsTest {
    private static final String URL_PATH = "/rest/analytics/query/execute";

    @Test
    public void testCreateClientSession() {
        String host = "localhost";
        int port = 8080;
        AnalyticsManagerSession clientSession1 = AnalyticsManagerOptions.createClientSession(host, port);
        AnalyticsManagerSession clientSession2 = AnalyticsManagerOptions.createClientSession(port);
        Assert.assertEquals(clientSession1.getServer().toString(), clientSession2.getServer().toString());
        Assert.assertEquals(clientSession1.getServer().toString(), "http://localhost:8080");
    }

    @Test
    public void testServerResolve() {
        String host = "localhost";
        int port = 8080;
        AnalyticsManagerSession clientSession = AnalyticsManagerOptions.createClientSession(host, port);
        String fullUri = clientSession.getServer().resolve(URL_PATH).toString();
        Assert.assertEquals(fullUri, "http://localhost:8080/rest/analytics/query/execute");
    }

}
