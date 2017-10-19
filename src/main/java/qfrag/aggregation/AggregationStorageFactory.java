package qfrag.aggregation;

import qfrag.conf.Configuration;

public class AggregationStorageFactory {

    public AggregationStorage createAggregationStorage(String name) {
        AggregationStorageMetadata metadata = Configuration.get().getAggregationMetadata(name);

        return createAggregationStorage(name, metadata);
    }

    public AggregationStorage createAggregationStorage(String name, AggregationStorageMetadata metadata) {
        if (metadata == null) {
            throw new RuntimeException("Attempted to create unregistered aggregation storage");
        }

        Class<?> keyClass = metadata.getKeyClass();

        return null;
    }
}
