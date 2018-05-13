package com.amannmalik.tabulardatatools.gateway;

import java.net.URI;
import java.util.List;

public interface DatabaseGateway {
    void register(URI uri, List<String> columnLabels);
    void unregister(URI uri);
}
