# XNAT Distributed Events Plugin README

The XNAT distributed events plugin enables XNAT to propagate events across multiple nodes in a distributed configuration. It uses ActiveMQ's Publish/Subscribe messaging to make critical system events that occur on a single node available to other nodes in a distributed configuration.

> **Note:** The distributed events plugin is intended to be deployed in a distributed XNAT configuration with a shared external ActiveMQ server. While installing this plugin shouldn't cause any issues in a single-XNAT deployment, it won't provide any functionality in that environment.

## Building

To build the distributed events plugin, run the following command:

```bash
./gradlew clean xnatPluginJar
```

You can find the newly built plugin jar at:

```bash
build/libs/dist-events-1.0.0-SNAPSHOT-xpl.jar
```

## Requirements

To take advantage of the distributed events functionality, you should have:

* A [multi-node XNAT configuration](https://wiki.xnat.org/documentation/multiple-web-front-ends-to-a-single-xnat-database-) (verify that each node in your multi-node configuration has its own distinct node ID configured in `${XNAT_HOME}/config/node-conf.properties`)
* An external stand-alone [ActiveMQ server](https://activemq.apache.org)
* If your multi-node configuration is using a front-end load balancer such as [nginx](https://nginx.org) or [traefik](https://traefik.io/traefik), you should make sure it's using [sticky](https://docs.nginx.com/nginx/admin-guide/load-balancer/http-load-balancer/#choosing-a-load-balancing-method) [sessions](https://traefik.io/glossary/what-are-sticky-sessions)

## Deploying

To deploy the distributed events plugin, perform the following procedure for each XNAT server in your deployment:

1. Shut down Tomcat
2. Copy the plugin jar to the `plugins` folder for your XNAT server(s)
3. Restart Tomcat

Once you've installed the plugin to each of your servers, you're ready to being using distributed events.
