package com.amannmalik.tabulardatatools.gateway;

import com.amannmalik.tabulardatatools.config.ColumnDefinition;

import java.net.URI;
import java.util.List;

public interface DatabaseGateway {
    void register(URI uri, List<ColumnDefinition> columnDefinitions);
    void unregister(URI uri);
}
