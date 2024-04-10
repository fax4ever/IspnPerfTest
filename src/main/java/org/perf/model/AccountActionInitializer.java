package org.perf.model;

import org.infinispan.protostream.GeneratedSchema;
import org.infinispan.protostream.annotations.AutoProtoSchemaBuilder;

@AutoProtoSchemaBuilder(
        includeClasses = {
                Account.class
        },
        schemaFileName = "account.proto",
        schemaFilePath = "proto/",
        schemaPackageName = "accountschema")
public interface AccountActionInitializer extends GeneratedSchema {

    GeneratedSchema INSTANCE = new AccountActionInitializerImpl();

}
