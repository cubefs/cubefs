# CubeFS Security Process Email Templates

This is a collection of email templates to handle various situations the security team encounters.

## Upcoming security release

```
Subject: Upcoming security release of CubeFS $VERSION
To: security@cubefs.groups.io

Hello CubeFS Community,

The CubeFS Product Security Committee and maintainers would like to announce the forthcoming release
of CubeFS $VERSION.

This release will be made available on the $ORDINALDAY of $MONTH $YEAR at
$PDTHOUR PDT ($GMTHOUR GMT). This release will fix $NUMDEFECTS security
defect(s). The highest rated security defect is considered $SEVERITY severity.

No further details or patches will be made available in advance of the release.

**Thanks**

Thanks to $REPORTER, $DEVELOPERS, and the $RELEASELEADS for the coordination is making this release.

Thanks,

$PERSON on behalf of the CubeFS Product Security Committee and maintainers
```

## Security Fix Announcement

```
Subject: Security release of CubeFS $VERSION is now available
To: security@cubefs.groups.io

Hello CubeFS Community,

The Product Security Committee and maintainers would like to announce the availability of CubeFS $VERSION.
This addresses the following CVE(s):

* CVE-YEAR-ABCDEF (CVSS score $CVSS): $CVESUMMARY
...

Upgrading to $VERSION is encouraged to fix these issues.

**Am I vulnerable?**

Run `cfs-server -v` and if it indicates a base version of $OLDVERSION or
older that means it is a vulnerable version.

<!-- Provide details on features, extensions, configuration that make it likely that a system is
vulnerable in practice. -->

**How do I mitigate the vulnerability?**

<!--
[This is an optional section. Remove if there are no mitigations.]
-->

**How do I upgrade?**

Follow the upgrade instructions at https://cubefs.io/docs/master/overview/introduction.html

**Vulnerability Details**

<!--
[For each CVE]
-->

***CVE-YEAR-ABCDEF***

$CVESUMMARY

This issue is filed as $CVE. We have rated it as [$CVSSSTRING]($CVSSURL)
($CVSS, $SEVERITY) [See the GitHub issue for more details]($GITHUBISSUEURL)

**Thanks**

Thanks to $REPORTER, $DEVELOPERS, and the $RELEASELEADS for the
coordination in making this release.

Thanks,

$PERSON on behalf of the CubeFS Product Security Committee and maintainers
```
