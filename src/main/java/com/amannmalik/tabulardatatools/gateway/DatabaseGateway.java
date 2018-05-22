package com.amannmalik.tabulardatatools.gateway;

import com.amannmalik.tabulardatatools.config.ColumnSpecification;

import java.net.URI;
import java.util.List;

public interface DatabaseGateway {
    void register(URI uri, List<ColumnSpecification> columnSpecifications);
    void unregister(URI uri);
}
