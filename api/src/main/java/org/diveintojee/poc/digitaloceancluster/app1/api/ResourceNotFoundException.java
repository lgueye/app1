package org.diveintojee.poc.digitaloceancluster.app1.api;

public class ResourceNotFoundException extends RuntimeException {
    public ResourceNotFoundException(String message) {
        super(message);
    }
}
