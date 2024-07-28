# CubeFS Governance

## Principles

The CubeFS community adheres to the following principles:

- Open: CubeFS is open source.
- Welcoming and respectful: See [Code of Conduct](CODE_OF_CONDUCT.md).
- Transparent and accessible: Changes to the CubeFS organization, CubeFS code repositories, and CNCF related activities (e.g. level, involvement, etc) are done in public.
- Merit: Ideas and contributions are accepted according to their technical merit and alignment with project objectives, scope, and design principles.

## Expectations from The Steering Committee

The CubeFS project has a Steering Committee.
- The term of the Steering Committee position is one year, and individuals can serve multiple terms in this role.
- Steering Committee Member is responsible for the top-level design of projects, formulation of roadmaps, and community management
- The Steering Committee member is the core maintainer.
- The composition of the Steering Committee members consists of core maintainers from independent developers or vendors. 
- To ensure fairness, efforts will be made to maintain a balanced representation among the personnel from different vendors.
- No single vendor can exceed 50% of the total number of personnel.
- The term of the Steering Committee is one year.

## Changes in Steering Committee

Changes in steering committee members are initiated by opening a github PR.

Anyone from CubeFS maintainers can vote on the PR with either +1 or -1.

Only the following votes are binding:
1) Any maintainer that has been listed in the top-level [MAINTAINERS](MAINTAINERS.md) file before the PR is opened.
2) Any maintainer from an organization may cast the vote for that organization. However, no organization
should have more binding votes than 1/2 of the total number of maintainers defined in 1).

The PR should only be opened no earlier than 4 weeks before the end of the term.
The PR should be kept open for no less than 2 weeks. The PR can only be merged after the end of the
last term, with more +1 than -1 in the binding votes.

When there are conflicting PRs about changes in Steering Committee, the PR with the most binding +1 votes is merged.

The Steering Committee member can volunteer to step down.

## Expectations from Maintainers

Every one carries water...

Making a community work requires input/effort from everyone. Maintainers should actively
participate in Pull Request reviews. Maintainers are expected to respond to assigned Pull Requests
in a *reasonable* time frame, either providing insights, or assign the Pull Requests to other
maintainers.
Every Maintainer is listed in the top-level [MAINTAINERS](MAINTAINERS.md) file.

## Becoming a Maintainer

On the successful merge of a significant pull request or upon making significant contributions,
any current maintainer can reach to the author behind the pull request and ask them if they are willing to become a CubeFS maintainer.
Maintainers have the authority to nominate new maintainers by either sending an email to maintainers@cubefs.groups.io or opening a pull request. 
Typically, new maintainers are selected from among the committers.The steering committee will review the proposal by majority votes,
and you will receive an invitation letter from the community after the review is passed.

- Project Governance: Setting policies and guidelines for the project, including contribution guidelines and code of conduct.
- Roadmap and Strategy: Propose the project roadmap and strategic direction, which includes deciding on the features to be developed and the priorities for the project.
- Release Management: Managing the release process, including setting release schedules, coordinating release activities, and ensuring that releases are properly tested and documented.
- Community Engagement: Building and fostering a positive community around the project, which includes welcoming new contributors, resolving conflicts, and promoting the project.
- Committer Management: Managing the committer group, which includes adding new committers, removing inactive ones, and handling disputes within the team.

## Changes in Maintainership

If a Maintainer feels she/he can not fulfill the "Expectations from Maintainers", they are free to step down.
The steering committee will adjust the list of maintainers based on the following factors
- The activity level and contribution level of the maintainer in the past six months.
- Balance of personnel across modules
- Module changes, such as additions or deprecations
- Balance of personnel among vendors.

In such a case:

A PR is required to move the person in question from the maintainer entry to the retirement entry of the respective OWNERS file. The person in question must be mentioned in the body of the PR. This acts as a final contact attempt so that they can provide their feedback.

Only for core maintainers who are losing their status:
remove them from the core-maintainers team;
go to https://maintainers.cncf.io/ and open a PR to remove them under CubeFS;
remove them from the cubefs.groups.io/g/maintainers mailing list.

## Expectations from Committers

- Code Contributions: Writing and committing code changes to the project repository.
- Code Reviews: Reviewing and approving code changes proposed by other contributors before they are merged into the main branch.
- Documentation: Updating and maintaining documentation related to the project.
- Bugs and Issues: Helping to triage and resolve bugs and other issues reported by users.
- Feature Development: Implementing new features based on the project roadmap and community feedback

Every Committer is listed in the top-level [MAINTAINERS](MAINTAINERS.md) file.

## Committer

Committer is an active contributor in the community who continuously makes contributions to the community by contributing codes, documentation, participating in community discussions, or answering community questions, etc. 

Typically, they need to have a good understanding of the project to help more community users quickly join the project. Committer will be responsible for reviewing relevant issues or PRs, and your opinions are also extremely important to the community.

Every Committer is listed in the top-level [MAINTAINERS](MAINTAINERS.md) file.

## Becoming a committer

If you are interested in becoming a committer, please email `maintainers@cubefs.groups.io` or opening a pull request,
and list your contributions.The steering committee will review the proposal by majority votes,
and you will receive an invitation letter from the community after the review is passed.

## Changes in Commitership

If a committer feels she/he can not fulfill the "Expectations from Maintainers", they are free to step down.
The steering committee will adjust the list of maintainers based on the following factors
- The activity level and contribution level of the committer in the past six months.
- Balance of personnel across modules
- Module changes, such as additions or deprecations
- Balance of personnel among vendors.

In such a case:

A PR is required to move the person in question from the committer entry to the retirement entry of the respective OWNERS file. The person in question must be mentioned in the body of the PR. This acts as a final contact attempt so that they can provide their feedback.

Only for core maintainers who are losing their status:
remove them from the core-maintainers team;
go to https://maintainers.cncf.io/ and open a PR to remove them under CubeFS;
remove them from the cubefs.groups.io/g/maintainers mailing list.

## Changes in Project Governance

- Changes in project governance (GOVERNANCE.md) could be initiated by opening a github PR.
- Anyone from CubeFS Steering Committee can vote on the PR with either +1 or -1.
- The PR should be kept open for no less than 2 weeks. 

## Roadmap

### Rules
  - Define goals: Clearly articulate the long-term and short-term objectives of the project. These goals should align with the project's vision and values while meeting the needs of users or the community.
  - Prioritize: Rank the goals based on their importance and urgency. Considering resource and time constraints, ensure that the focus is placed on the most critical objectives.
  - Time-based planning: Break down the goals into milestones or phased tasks. Define specific objectives and measurable metrics for each phase to evaluate progress later on.
  - Transparency and communication: Share the roadmap openly with project stakeholders, including users, contributors, and other interested parties. This helps establish transparency and keeps everyone informed about the project's direction and plans.
  - Continuous adjustment: An open-source project roadmap should be a dynamic document that may require adjustments and updates over time as new insights are gained. Regularly review and reassess the roadmap, making modifications as needed.
  - Approach: When creating the roadmap, take into account the opinions and suggestions of the community to ensure that the project's development aligns with broad expectations and needs.
  - Engage in discussions with project users and contributors to gather feedback and understand their requirements. This can be done through channels such as mailing lists, forums, and social media.

### Changes in project Roadmap
  - Changes in project Roadmap (ROADMAP.md) could be initiated by opening a github PR.
  - Anyone from CubeFS Maintainers can vote on the PR with either +1 or -1.
  - The PR should be kept open for no less than 2 weeks. 

## SIG

### Rules
- CubeFS project establishes Special Interest Groups (SIGs) based on the needs of project development. Each SIG is composed of individuals from multiple companies and organizations who share a common goal of advancing the project in specific areas. SIGs are permanent unless dissolved by the Technical Steering Committee (TSC).

- The objective is to achieve a distributed decision-making structure and code ownership, and provide a dedicated forum to complete work, make decisions, and onboard new contributors. Each identifiable part of the project (such as repositories, subdirectories, APIs, tests, issues, PRs) is intended to be owned by a specific SIG. 
SIGs must have an open membership policy and always operate in an open environment. Changes in the leadership of SIGs (SIG chairs and technical leads) require approval from the Technical Steering Committee and can have varying durations.

- SIG Chairs:
Each SIG must have at least one SIG chair, and there can be a maximum of two chairs simultaneously. SIG chairs are organizers and advocates responsible for the operation of the SIG, as well as communication and coordination with other SIGs and the broader community.

- Technical Leads:
Technical leads are responsible for leading the SIG to align with its technical coordination. This coordination includes both internal coordination within the SIG and external coordination across the entire project.


### Member Changes and Management in SIGs
- SIGs can independently decide whether to include technical leads in their charters. Depending on the overall size of the SIG, SIG chairs may nominate approximately two to three individuals to support the technical aspects of the group. In order to fulfill their responsibilities, technical leads should have the same authority as the chairs but need apply chairs's option if there's any controversies.
- The maintainer of a corresponding module is required to be part of the respective SIG group. Currently, we are gradually implementing SIG grouping. Maintainers still have full authority over the modules they are responsible for. Any disputes between the maintainer and the SIG group can be resolved through the TSC (Technical Steering Committee).
- The chairperson must be a maintainer member.This helps to reduce controversies regarding SIG governance strategies and overall community governance.
- PR applications should be submitted by maintainers and approved by the TSC once the chair or tec lead change. 
- The internal management of SIGs can be determined by the SIG chairs according to the rules established by SIGs themselves.

## Vendor-neutrality

- Vendors share communication channels of the community, such as social media and messaging platforms.
- All vendors involved in the project are encouraged to actively participate in the topic selection process for public events.
- Vendors can apply to participate in open-source conferences and events. The invitation should be cc'ed to `maintainers@cubefs.groups.io`.
- The Steering Committee will review the application, and if it is approved, the Steering Committee can provide guidance on this matter.

## Decision making process

Decisions are build on consensus between maintainers.
Proposals and ideas can either be submitted for agreement via a github issue or PR,
or by sending an email to `maintainers@cubefs.groups.io`.

In general, we prefer that technical issues and maintainer membership are amicably worked out between the persons involved.
If a dispute cannot be decided independently, get a third-party maintainer (e.g. a mutual contact with some background
on the issue, but not involved in the conflict) to intercede.
If a dispute still cannot be decided, the Steering Committee can make the decision by majority votes.

Decision making process should be transparent to adhere to
the principles of CubeFS project.

All proposals, ideas, and decisions by maintainers or the Steering Committee should either be part of a github issue or PR, or be sent to `maintainers@cubefs.groups.io`.

## Github Project Administration

The __cubefs__ GitHub project maintainers team reflects the list of Maintainers.

## Other Projects

The CubeFS organization is open to receive new sub-projects under its umbrella. To accept a project
into the __CubeFS__ organization, it has to meet the following criteria:

- Must be licensed under the terms of the Apache License v2.0
- Must be related to one or more scopes of the CubeFS ecosystem:
  - CubeFS project artifacts (website, deployments, CI, etc)
  - External plugins
  - Other storage related topics
- Must be supported by a Maintainer not associated or affiliated with the author(s) of the sub-projects

The submission process starts as a Pull Request or Issue on the
[cubefs/cubefs](https://github.com/cubefs/cubefs) repository with the required information
mentioned above. Once a project is accepted, it's considered a __sub-project under the umbrella of CubeFS__.

## New Plugins

The CubeFS is open to receive new plugins as part of the CubeFS repo. The submission process is the same as a Pull Request submission. Unlike small Pull Requests though, a new plugin submission should only be approved by a maintainer not associated or affiliated with the author(s) of the plugin.

## CubeFS and CNCF

CubeFS might be involved in CNCF (or other CNCF projects) related
marketing, events, or activities. Any maintainer could help driving the CubeFS involvement, as long as
she/he sends email to `maintainers@cubefs.groups.io` (or create a GitHub Pull Request) to call for participation
from other maintainers. The `Call for Participation` should be kept open for no less than a week if time
permits, or a _reasonable_ time frame to allow maintainers to have a chance to volunteer.

## Code of Conduct

The [CubeFS Code of Conduct](CODE_OF_CONDUCT.md) is aligned with the CNCF Code of Conduct.

## Liaison Officer for CNCF

The liaison officer is responsible for daily communication with CNCF, including information updates, demand communication, community activity meetings, etc.