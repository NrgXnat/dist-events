# XNAT Distributed Events Plugin Changelog

The XNAT distributed events plugin enables XNAT to propagate events across multiple nodes in a distributed configuration.

## Version 1.1.0

* **Improvement:** Propagate changes to prefs and config changes across all nodes, invalidating and updating any cached values across all nodes. Requires XNAT 1.9.2.2. 


## Version 1.0.0
[Released Apr 9, 2025](https://bitbucket.org/xnatx/dist-events/src/1.0.0/)

Initial open source release. Covers the following use cases:

* Creating and deleting projects
* Creating, deleting. moving, or sharing subjects
* Creating, deleting. moving, or sharing experiments
* Adding or deleting user roles

Installing this plugin on a multi-node XNAT setup allows a site admin to remedy the issues reported in [XNAT-6889](https://radiologics.atlassian.net/browse/XNAT-6889). 
