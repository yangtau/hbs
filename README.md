# HBS

The HBS project provides transactional support for HBase.

## Load from configuration
```xml
<property>
  <name>hbase.coprocessor.region.classes</name>
  <value>org.yangtau.hbs.hbase.coprocessor.GetEndpoint,org.yangtau.hbs.hbase.coprocessor.PutEndpoint</value>
</property>
```

