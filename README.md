# XNAT Distributed Events Plugin README

The XNAT distributed events plugin enables XNAT to propagate events across multiple nodes in a distributed configuration. It uses ActiveMQ's Publish/Subscribe messaging to make critical system events that occur on a single node available to other nodes in a distributed configuration.

## Building the plugin

To build the distributed events plugin, run the following command:

```bash
./gradlew clean xnatPluginJar
```
