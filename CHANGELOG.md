# XNAT Distributed Events Plugin Changelog

The XNAT distributed events plugin enables XNAT to propagate events across multiple nodes in a distributed configuration.


## Version 2.0.1
Not yet released

Requires XNAT 1.10.0 or higher

* **Bugfix:** Stop the Hibernate update listener from keeping every entity changed on other nodes. It loaded them into one Hibernate session that lasted as long as the application, with no transaction, so a node's heap filled until it ran out of memory. Every message failed with "Connection is closed" once the database closed that session's connection. Entities with large-object columns always failed on PostgreSQL with "Large Objects may not be used in auto-commit mode". The listener now evicts the node's cached copy first, and loads the entity only when a handler method needs it, in a short-lived read-only transaction. Handler methods now receive a detached entity.


## Version 2.0.0
[Released Apr 27, 2026](https://bitbucket.org/xnatx/dist-events/src/2.0.0/)

Requires XNAT 1.10.0 or higher

* **Improvement:** Propagate the creation of Dynamic Data Types and CRUD events related to instances of those data types across nodes
* **Improvement:** Update core code to function on JDK21 for compatibility with XNAT 1.10.0 and higher


## Version 1.2.2
[Released Jan 21, 2026](https://bitbucket.org/xnatx/dist-events/src/1.2.2/)

Requires XNAT 1.9.3 or higher

* **Bugfix:** Prevent preference change updates from overwhelming the ActiveMQ server



## Version 1.2.1
[Released Nov 26, 2025](https://bitbucket.org/xnatx/dist-events/src/1.2.1/)

Requires XNAT 1.9.3

* **Bugfix:** Prevent NPEs on certain update events. Requires XNAT 1.9.3


## Version 1.2.0
[Released Nov 21, 2025](https://bitbucket.org/xnatx/dist-events/src/1.2.0/)

Requires XNAT 1.9.3

* **Improvement:** Propagate changes to SCP Receivers across all nodes. Requires XNAT 1.9.3.


## Version 1.1.0
[Released Sep 16, 2025](https://bitbucket.org/xnatx/dist-events/src/1.1.0/)

Requires XNAT 1.9.2.2

* **Improvement:** Propagate changes to prefs and config changes across all nodes, invalidating and updating any cached values across all nodes. Requires XNAT 1.9.2.2. 


## Version 1.0.0
[Released Apr 9, 2025](https://bitbucket.org/xnatx/dist-events/src/1.0.0/)

Initial open source release. Covers the following use cases:

* Creating and deleting projects
* Creating, deleting. moving, or sharing subjects
* Creating, deleting. moving, or sharing experiments
* Adding or deleting user roles

Installing this plugin on a multi-node XNAT setup allows a site admin to remedy the issues reported in [XNAT-6889](https://radiologics.atlassian.net/browse/XNAT-6889). 
