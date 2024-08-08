# Self-assessment

The Self-assessment is the initial document for projects to begin thinking about the
security of the project, determining gaps in their security, and preparing any security
documentation for their users. This document is ideal for projects currently in the
CNCF **sandbox** as well as projects that are looking to receive a joint assessment and
currently in CNCF **incubation**.

For a detailed guide with step-by-step discussion and examples, check out the free
Express Learning course provided by Linux Foundation Training & Certification:
[Security Assessments for Open Source Projects](https://training.linuxfoundation.org/express-learning/security-self-assessments-for-open-source-projects-lfel1005/).

## Self-assessment outline

### Table of contents

* [Metadata](#metadata)
  * [Security links](#security-links)
* [Overview](#overview)
  * [Actors](#actors)
  * [Actions](#actions)
  * [Background](#background)
  * [Goals](#goals)
  * [Non-goals](#non-goals)
* [Self-assessment use](#self-assessment-use)
* [Security functions and features](#security-functions-and-features)
* [Project compliance](#project-compliance)
* [Secure development practices](#secure-development-practices)
* [Security issue resolution](#security-issue-resolution)
* [Appendix](#appendix)

### Metadata

A table at the top for quick reference information, later used for indexing.

|||
| -- | -- |
| Assessment Stage | Complete |
| Software |https://github.com/cubefs/cubefs  |
| Security Provider | No |
| Languages | Mainly Go language |
| SBOM | https://github.com/cubefs/cubefs/blob/master/go.mod |

#### Security links

<!-- markdown-link-check-disable -->

Provide the list of links to existing security documentation for the project. You may
use the table below as an example:

| Doc | url |
| -- | -- |
| Security file | https://github.com/cubefs/cubefs/blob/master/SECURITY.md |
| Default and optional configs | https://cubefs.io/docs/master/security/security_practice.html |

<!-- markdown-link-check-enable -->

### Overview

CubeFS is a file storage system that provides cloud-native file system capabilities and is compatible with object storage. Moreover, it offers a multitude of competitive features and capabilities.
The project’s differentiation is the main feature of CubeFS: [link to md#section](https://github.com/cubefs/cubefs/blob/master/README.md#what-can-you-build-with-cubefs)

#### Background

File storage and object storage are key in the storage domain.

* [CubeFS Introduction](https://cubefs.io/docs/master/overview/introduction.html) includes what CubeFS does, and why it does it
* Use Case: How CubeFS Accelerates AI training in Hybrid Cloud platform: [link](https://cubefs.io/blog/useCases/oppo-ai.html)
* Documentation:[Integration with Kubernetes](https://cubefs.io/docs/master/ecology/k8s.html)

#### Actors

CubeFS fundamentallly have five actors，includes

* **Client** : User side for business service
* **Master Node** : Cluster management
* **Auth Node** : Authentication management
* **Meta Node** : Metadata management
* **Data Node** : Replica data management

#### Actions

According to [authnode check architecture](https://cubefs.io/docs/master/design/authnode.html#architecture) , CubeFS has the authentication mechanism to confirm the safety of data transfer. Based on the architecture, messages over HTTPS and TCP can be encrypted and decrypted by the endpoint.Now the mechanism is mainly used between the client and the master node, as the volume information and management are much more important.

#### Goals

CubeFS has planned the  [future work](https://cubefs.io/docs/master/design/authnode.html#future-work)  to do, which are our goals.

#### Non-goals


### Self-assessment use

This self-assessment is created by the CubeFS team to perform an internal analysis of the
project's security.  It is not intended to provide a security audit of CubeFS , or
function as an independent assessment or attestation of CubeFS 's security health.

This document serves to provide CubeFS  users with an initial understanding of
CubeFS 's security, where to find existing security documentation, CubeFS  plans for
security, and general overview of CubeFS  security practices, both for development of
CubeFS  as well as security of CubeFS .

This document provides the CNCF TAG-Security with an initial understanding of CubeFS
to assist in a joint-assessment, necessary for projects under incubation.  Taken
together, this document and the joint-assessment serve as a cornerstone for if and when
CubeFS  seeks graduation and is preparing for a security audit.

### Security functions and features

* Critical. According to the [guide documentation](https://cubefs.io/docs/master/user-guide/authnode.html),  CubeFS adopted MITM threat module, Suggest user start CubeFS with AuthNode step by step， thus allows the cluster and volume management to be protected by authentication; this is a basic security method, especially for multi-tenant clusters.
* Security Relevant.  A listing of security relevant components of the project with
  brief description.  These are considered important to enhance the overall security of
  the project, such as deployment configurations, settings, etc.  These should also be
  included in threat modeling.
  1. Don't use the owner information directly which have super access, Ceate user and grant the authetiction to mount the volume accoring to [USER Management](https://cubefs.io/docs/master/dev-guide/admin-api/master/user.html#create-user)
  2. Massive users authetication management can reference to [ladp+autoMount](https://cubefs.io/docs/release-3.3.2/maintenance/security_practice.html#ladp-automount),
  3. The master IP is not directly exposed, and a domain name approach is used through a gateway.
  4. Traffic QoS for volumes is crucial for ensuring the stable operation of the system : [reference documentation](https://cubefs.io/docs/master/dev-guide/admin-api/master/volume.html#qos-flow-control-parameters-and-interfaces)
  5. Limiting requests to the master interface by QoS is essential when many clients are online : [reference documentation](https://cubefs.io/docs/master/feature/qos.html)
  6. S3 Authentication Capability : [reference documentation](https://cubefs.io/docs/master/security/security_practice.html#s3-authentication-capability)


### Project compliance

* Compliance.  List any security standards or sub-sections the project is
  already documented as meeting (PCI-DSS, COBIT, ISO, GDPR, etc.).

### Secure development practices

* Development Pipeline.  A description of the testing and assessment processes that
  the software undergoes as it is developed and built. Be sure to include specific
  information such as if contributors are required to sign commits, if any container
  images immutable and signed, how many reviewers before merging, any automated checks for
  vulnerabilities, etc.
  
  * Typically, commits must include test cases and coverage for most processes, and GitHub will notify users of any coverage gaps
  * Code submissions on GitHub undergo multiple layers of checks.CubeFS have integrated many tools in CI, includes ci-test-unit,ci-test-s3 and ci-sast : [reference link](https://github.com/cubefs/cubefs/blob/master/.github/workflows/ci.yml)
  * Contributor make the commit message need comply the rules or else can't pass the pull request t : [reference link](check.https://github.com/cubefs/cubefs/blob/master/CONTRIBUTING.md#commit-message-guidelines)
* Communication Channels.  Reference where you document how to reach your team or
  describe in corresponding section.
  
  * Internal. As most of the maintainers and committers are chinese, we use wechat communicate with each other: [reference link](https://github.com/cubefs/cubefs/issues/604)
  * Inbound. How do users or prospective users communicate with the team?
    
    * Users can communicate with the team by joining WeChat or Slack directly:[reference link](https://github.com/cubefs/cubefs/issues/604)
    * Feedback and suggestions can also be submitted via Github Issues: [reference link](https://github.com/cubefs/cubefs/issues)
    * Users can propose project suggestions and initiate public discussions through the discussion portal:[reference link](https://github.com/cubefs/cubefs/discussions)
    * Additionally, we have an email system; specific information is available.
  * Users can communicate with the team by joining WeChat or Slack directly.
  * Feedback and suggestions can also be submitted via Issues.
* Outbound. How do you communicate with your users? (e.g. flibble-announce@
  mailing list)
  
  * WeChat or Slack:[WeChat reference link](https://github.com/cubefs/cubefs/issues/604),[cubefs.slack.com](https://cubefs.slack.com/)
  * Portal website:[https://cubefs.io](https://cubefs.io/)
  * Mailing list:[users@cubefs.groups.io](mailto:users@cubefs.groups.io)
* Ecosystem. How does your software fit into the cloud native ecosystem?
  
  * CubeFS Container Storage Interface (CSI) plugins: [reference link](https://github.com/cubefs/cubefs-csi)
  * The cubefs-helm project helps deploy a Cubefs cluster orchestrated by Kubernetes:[reference link](https://github.com/cubefs/cubefs-helm)

### Security issue resolution

* Responsible Disclosures Process.
  
  * [Disclosures Process ](https://github.com/cubefs/cubefs/blob/master/security/security-release-process.md#disclosures)
  * CubeFS security issue can also be reported through the portal [##Open a draft security advisory](https://github.com/cubefs/cubefs/security/advisories/new).
* Vulnerability Response Process.
  
  * [Securty committe memeber](https://github.com/cubefs/cubefs/blob/master/security/security-release-process.md#product-security-committee-psc) will review it through portal [## Security Advisories Triage](https://github.com/cubefs/cubefs/security/advisories?state=triage), once the issue be resolved   it will be published at portal [## Security Advisories published](https://github.com/cubefs/cubefs/security/advisories?state=published)
* Incident Response. A description of the defined procedures for triage,
  confirmation, notification of vulnerability or security incident, and
  patching/update availability.
  
  According to [CubeFS security  committee's duty description](https://github.com/cubefs/cubefs/blob/master/security/security-release-process.md#product-security-committee-psc)， the definitions are list below:
  
  * Triage: make sure the people who should be in "the know" (aka notified) are notified, also responds to issues that are not actually issues and let the CubeFS maintainers know that. This person is the escalation path for a bug if it is one.
  * Infra: make sure we can test the fixes appropriately.
  * Disclosure: handles public messaging around the bug. Documentation on how to upgrade. Changelog. Explaining to public the severity. notifications of bugs sent to mailing lists etc. Requests CVEs.
  * Release: Create new release addressing a security fix.

### Appendix

* Known Issues Over Time. List or summarize statistics of past vulnerabilities
  with links. If none have been reported, provide data, if any, about your track
  record in catching issues in code review or automated testing.
  
  * CubeFS has provided portal of [security overview](https://github.com/orgs/cubefs/security/overview?period=last90days), where the detailed trend charts available.
* [Open SSF Best Practices](https://www.bestpractices.dev/en).
  Best Practices. A brief discussion of where the project is at
  with respect to CII best practices and what it would need to
  achieve the badge.
  
  * CubeFS passed the OpenSSF Best Practices two years ago: [reference link](https://www.bestpractices.dev/zh-CN/projects/6232). Since then, CubeFS has made improvements for better management. Now, CubeFS is working towards achieving the silver badge.
* Case Studies. Provide context for reviewers by detailing 2-3 scenarios of
  real-world use cases.
  
  * [CubeFS Third part security audit](https://github.com/cubefs/cubefs/blob/master/security/CubeFS-security-audit-2023-report.pdf) report some issues and already been fixed，and the [case](https://github.com/cubefs/cubefs/security/advisories?state=Triage) finially be published now.
* Related Projects / Vendors. Reflect on times prospective users have asked
  about the differences between your project and projectX. Reviewers will have
  the same question.
  
  * File storage and object storage are key in the storage domain. CubeFS is a file storage system that provides cloud-native file system capabilities and is compatible with object storage. Moreover, it offers a multitude of competitive features and capabilities.
  * The project’s differentiation is the main feature of CubeFS: [link to md#section](https://github.com/cubefs/cubefs/blob/master/README.md#what-can-you-build-with-cubefs)
  * Use Case: How CubeFS Accelerates AI training in Hybrid Cloud platform: [link](https://cubefs.io/blog/useCases/oppo-ai.html)
  * Well-Rounded [Roadmap Schedule](https://github.com/cubefs/cubefs/blob/master/ROADMAP.md) to adapt CubeFS to the requirements of multiple scenarios.
