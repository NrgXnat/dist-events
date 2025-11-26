# XNAT Distributed Events Plugin Changelog

The XNAT distributed events plugin enables XNAT to propagate events across multiple nodes in a distributed configuration.

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
