# CubeFS Releases

This repo contains the tooling and documentation for the release of
the core CubeFS project.

In the future it is expected that the functionality will expand and be
generalized to support release infrastructure for all of the CubeFS
sub-projects as well.

The tooling and doc here are expected to change often as requirements
change and the project(s) evolve.

The doc and tooling in this repo are NOT designed to address the planning,
coordination of releases.  For more info on feature and release planning see:
* [CubeFS Roadmap](https://github.com/cubefs/cubefs/blob/master/ROADMAP.md)


## Types of Releases

* Beta releases (`vX.Y.Z-beta.W`) are cut from their respective release branch,
  `release-X.Y.Z`.
* Official releases (`vX.Y.Z`) are cut from their respective release branch,
  `release-X.Y.Z`.

## Release Schedule

| Type      | Versioning     | Branch               | Frequency                    |
| ----      | ----------     | ---------            | ---------                    |
| beta      | vX.Y.Z-beta    | release-X.Y.Z-Beta   | as needed (at branch time)   |
| official  | vX.Y.Z         | release-X.Y.Z        | as needed (post beta)        |

# Types of Branch 
* Develop Branch (`develop-X.Y.Z`)
* Release Branch (`release-X.Y.Z`)

# Versioning support by CubeFS
This document describes the versions supported by the CubeFS project.

Service versioning and supported versions
CubeFS versions are expressed as x.y.z, where x is the major version, y is the minor version, and z is the patch version, following Semantic Versioning terminology. New minor versions may add additional features to the API.

The CubeFS project maintains release branches for the current version and previous release. For example, when v3.3.* is the current version, v3.1.* is supported. When v3.4.* is released, v3.1.* goes out of support.

The project Maintainers own this decision.

# Artifacts included in the release
## Binary file version
- cubefs-3.3.2-linux-amd64.tar.gz
- cubefs-3.3.2-linux-amd64.tar.gz.sha256sum

## Source Code Related
- Generate a key pair based on GPG encryption to ensure that the code is not maliciously modified, for example：v3.3.2.tar.gz.asc
- Source code archive types
	- Source code(zip)
	- Source code(tar.gz)
