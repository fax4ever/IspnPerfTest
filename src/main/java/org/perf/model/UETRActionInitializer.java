package org.perf.model;

import org.infinispan.protostream.GeneratedSchema;
import org.infinispan.protostream.annotations.AutoProtoSchemaBuilder;

@AutoProtoSchemaBuilder(
        includeClasses = {
                UETRAction.class
        },
        schemaFileName = "uetraction.proto",
        schemaFilePath = "proto/",
        schemaPackageName = "uetraction")
public interface UETRActionInitializer extends GeneratedSchema {

        GeneratedSchema INSTANCE = new UETRActionInitializerImpl();

}
