package com.amannmalik.tabulardatatools.gateway;

import java.net.URI;
import java.nio.file.Path;

public interface StorageGateway {
    void get(URI uri, Path path);
    void put(URI uri, Path path);
    void delete(URI uri);
}
