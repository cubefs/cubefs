# CubeFS Governance

## Principles

The CubeFS community adheres to the following principles:

- Open: CubeFS is open source.
- Welcoming and respectful: See [Code of Conduct](CODE_OF_CONDUCT.md).
- Transparent and accessible: Changes to the CubeFS organization, CubeFS code repositories, and CNCF related activities (e.g. level, involvement, etc) are done in public.
- Merit: Ideas and contributions are accepted according to their technical merit and alignment with project objectives, scope, and design principles.

## Expectations from The Technical Steering Committee(TSC)

The CubeFS TSC is the governing body for the CubeFS project
- Providing decision-making and oversight related to its bylaws. 
- Defines the project's values, structure. 
- Responsible for the top-level design of projects, formulation of roadmaps, and community management.
- Approve the creation and dissolution of SIGs
- Establish and maintain the overall technical governance guidelines for the project.
- Make decisions by majority vote if consensus cannot be reached.

##  The structure of the Technical Steering Committee

- The TSC from independent developers and vendors.
- To ensure fairness, efforts will be made to maintain a balanced representation among the personnel from different vendors.
- No single vendor can exceed 50% of the total number of personnel.
- The term of the TSC is two years.
- The TSC consists of 5 individuals.

##  The TSC decision-making process

During the first year of the TSC term, if a TSC meeting lacks a quorum, an email voting procedure will be initiated. If email voting also fails to achieve a quorum within a week, the motion will be considered approved unless there are outstanding objections from TSC members.

## Becoming a TSC Member

The term of the TSC position is two years, and individuals can serve multiple terms in this role.
Changes in TSC members are initiated by opening a github PR.

- **Criteria**: Nominees must be a maintainer, should have made significant contributions to the community during the last 2 years.Have sufficient reputation and influence in the community.
- **Nominations**:
	- Each individual in a Selecting Group may nominate up to 2 people, at most 1 of whom may be from the same group of Related Companies. Each nominee must agree to participate prior to being added to the nomination list. Self nomination is allowed. Each nominee needs 2 more contributors’ endorsements from different organizations/companies.
	- Nominations must be made public and should include the nominee's name, contact information, and a statement of experience.Current TSC shall determine the process and timeline for the nominations, qualification and election of new TSC members.
- **Elections**:
	- If the number of Qualified Nominees is equal to or less than the number of TSC seats available to be elected, the Qualified Nominees shall be approved after the nomination period has closed. 
	- If there are more Qualified Nominees than open TSC seats available for election, then the Selecting Group shall elect the TSC members via a Condorcet vote. 
	- A Condorcet vote shall be run using the Condorcet-IRV method through the Cornell online service (https://civs.cs.cornell.edu/).
	- Only the following votes are binding:
		- Any maintainer that has been listed in the top-level [MAINTAINERS](MAINTAINERS.md) file before the PR is opened.
		- Any maintainer from an organization may cast the vote for that organization. However, no organization should have more binding votes than 1/2 of the total number of maintainers defined in 1).

The PR should only be opened no earlier than 4 weeks before the end of the term.
The PR should be kept open for no less than 2 weeks. The PR can only be merged after the end of the
last term, with more +1 than -1 in the binding votes.

The TSC will decide on and publish an election process within 3 months of formalizing this organizational structure. This will cover voting eligibility, eligibility for candidacy, the election process and schedule.

## Changes in TSC
- If a TSC member feels she/he can not fulfill the "Expectations from TSC", they are free to step down.

In such a case:
- A PR is required to update the person's TSC role in question to that of a normal maintainer.
- PR should be reviewed by TSC member.
- TSC member nominations can begin in 2 weeks, add a new TSC member and ensure the normal operation of the TSC.

## Changes in Project Governance

- Changes in project governance (GOVERNANCE.md) could be initiated by opening a github PR.
- Anyone from the CubeFS TSC can vote on the PR with either +1 or -1, and it will pass if it receives a majority of votes.
- The PR should be kept open for no less than 2 weeks. 

## Expectations from Maintainers

- Project Governance: Propose the policies and guidelines for the project, including contribution guidelines and code of conduct.
- Roadmap and Strategy: Propose the project roadmap and strategic direction, which includes deciding on the features to be developed and the priorities for the project.
- Release Management: Managing the release process, including setting release schedules, coordinating release activities, and ensuring that releases are properly tested and documented.
- Community Engagement: Building and fostering a positive community around the project, which includes welcoming new contributors, resolving conflicts, and promoting the project.
- Committer Management: Managing the committer group, which includes adding new committers, removing inactive ones, and handling disputes within the team.

Every Maintainer is listed in the top-level [MAINTAINERS](MAINTAINERS.md) file.

## The structure of the Maintainers
- The composition of the Maintainers consists from independent developers or vendors.
- No single vendor can exceed 50% of the total number of personnel.
- The Maintainers consists of 10~15 individuals.

## Becoming a Maintainer

- **Criteria**: Upon the successful merge of a significant pull request or upon making significant contributions,any current maintainer can reach to the author behind the pull request and ask them if they are willing to become a CubeFS maintainer.
- **Nominate**: Maintainers have the authority to nominate new maintainers by either sending an email to maintainers@cubefs.groups.io or opening a pull request. 
- **Elections**: Typically, new maintainers are selected from among the committers.The TSC will review the proposal by majority votes,and you will receive an invitation letter from the community after the review is passed.

## Changes in Maintainership

- If a Maintainer feels she/he can not fulfill the "Expectations from Maintainers", they are free to step down.
- The TSC will adjust the list of maintainers based on the following factors
	- The activity level and contribution level of the maintainer in the past six months.
	- Balance of personnel across modules
	- Module changes, such as additions or deprecations
	- Balance of personnel among vendors.

In such a case:
- A PR is required to move the person in question from the maintainer entry to the retirement entry of the MAINTAINERS.md. This acts as a final contact attempt so that they can provide their feedback.
- Maintainers who are losing their status:
	- Go to https://maintainers.cncf.io/ and open a PR to remove them under CubeFS;
	- Remove them from the cubefs.groups.io/g/maintainers mailing list.

## Expectations from Committers

- Code Contributions: Writing and committing code changes to the project repository.
- Code Reviews: Reviewing and approving code changes proposed by other contributors before they are merged into the main branch.
- Documentation: Updating and maintaining documentation related to the project.
- Bugs and Issues: Helping to triage and resolve bugs and other issues reported by users.
- Feature Development: Implementing new features based on the project roadmap and community feedback

## The structure of the Committers
- The composition of the Committers consists from independent developers or vendors.
- No single vendor can exceed 50% of the total number of personnel.
- The Committers consists of 10~15 individuals.

## Becoming a committer
- **Criteria**: Upon the successful merging of a meaningful pull request or after making certain contributions
- **Nominate**:
	- Any current maintainer can reach to the author behind the pull request and ask them if they are willing to become a CubeFS committer.
	- Contributors can also nominate themselves to become committers, please email `maintainers@cubefs.groups.io` or opening a pull request,and list your contributions.
- **Elections**: The TSC will review the proposal by majority votes internally,and you will receive an invitation letter from the community after the review is passed.

## Changes in Commitership

- If a committer feels she/he can not fulfill the "Expectations from Maintainers", they are free to step down.
- The TSC will adjust the list of maintainers based on the following factors
	- The activity level and contribution level of the committer in the past six months.
	- Balance of personnel across modules
	- Module changes, such as additions or deprecations
	- Balance of personnel among vendors.

In such a case:
- A PR is required to move the person in question from the committer entry to the retirement entry of the MAINTAINERS.md. The person in question must be mentioned in the body of the PR. This acts as a final contact attempt so that they can provide their feedback.
- Remove them from the cubefs.groups.io/g/maintainers mailing list.

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
- TSC will collect all proposals from maintainers through internal meetings or the maintainers@cubefs.groups.io email group.
- Changes to the project roadmap (ROADMAP.md) should be initiated as a pull request on GitHub by a TSC member.
- TSC will make decisions based on a majority vote.
- The pull request should remain open for a minimum of 2 weeks.

## SIG
- Currently, CubeFS are gradually implementing [SIG grouping](https://github.com/cubefs/cubefs-community). 
- CubeFS project establishes Special Interest Groups (SIGs) based on the needs of project development.  

### Expectations from SIGs
- The objective is to achieve a distributed decision-making structure and code ownership
- Provide a dedicated forum to complete work, make decisions, and onboard new contributors. 
- Each identifiable part of the project (such as repositories, subdirectories, APIs, tests, issues, PRs) is intended to be owned by a specific SIG. 
- SIGs must have an open membership policy and always operate in an open environment. 

### Expectations from SIGs member
- SIG Chairs:
	- Responsibilities: 
		- SIG's organizers and advocates, responsible for the operation of the SIG, as well as for establishing development goals for the SIG.
		- Communication and coordination with other SIGs and the broader community.
	- Each SIG must have at least one SIG chair, and there can be a maximum of two chairs simultaneously.
- Technical Leads:
	- Responsibilities: Technical leads are responsible for leading the SIG to align with its technical coordination, especilly at technical decision-making and execution
	- SIG may have a technical lead or may not, depending on the nature of the SIG.
- Participants(member)
	- Responsibilities: Actively contribute to the SIG, finish the work assigned by the technical leader.

	
RelationShip between Chairs and Technical Leads: 
- Chairs are responsible for the goals and direction of the SIG, as well as for setting long-term plans and short-term objectives. 
- Technical leads are responsible for implementing the goals and short-term objectives. 
- In case of disputes, issues can be submitted to the TSC for decision-making.
	
### The RelationShip between SIG and other organization or role
- RelationShip with TSC
	- SIGs follow the leadership of the TSC.
- RelationShip with Maintainer in Maintainer list:
	- If the SIG is responsible for the main project module, all maintainers and committers associated with the module should be included in the maintainer list to prevent interruptions.
	- Any disputes between the maintainer and the SIG group can be resolved through the TSC.

### Becoming a member of SIGs
- **Criteria**:
	- The chairperson must be a maintainer member.This helps to reduce controversies regarding SIG governance strategies and overall community governance.
	- The Technical Leads must be a maintainer or a committer.
	- The participants(member) must be contributors who can actively contribute to the SIG.
- **Nominate**
	- SIG Chairs:
		- TSC members nominate maintainers internally via maintainers@cubefs.groups.io, and the nominations are approved by a majority vote, followed by a public review on GitHub by maintainers.
	- Technical Leads:
		- Depending on the overall size of the SIG, SIG chairs may nominate approximately two to three individuals to support the technical aspects of the group. 
		- SIG Chairs can independently decide whether to include technical leads in their charters. 
	- Participants:
		- No need to nominate, added directly by chair or technical leader.
- **Elections**
	- Nominate PR applications should be submitted by maintainers and approved by the TSC when there is a change in the chair or tech lead. 
	
### Member Changes and Management in SIGs
- If a chair or tec leader feels she/he can not fulfill the "Expectations from SIGs", they are free to step down.
- The TSC will adjust the chair or tec leader based on the following factors
	- The activity level and contribution level in the past six months.
	- Balance of personnel among vendors.
- If participants cannot meet the requirements of the SIG, they can be removed directly by the chair or technical leader by opening a PR in the SIG and notifying the participant.

### SIG Lifecycle
- Formation:
   - Any community maintainers and contributors can propose the creation of a SIG..
   - Community discussion and feedback are gathered.
   - Each SIG is composed of individuals from multiple companies and organizations who share a common goal of advancing the project in specific areas.
- Approval:
   - The proposal is reviewed by the TSC to ensure it aligns with the project's objectives, make decisions by majority vote.
   - If approved, the SIG is officially established, and members can be recruited.
- Active Phase:
   - The SIG defines its goals, responsibilities, and processes.
   - Members are recruited and roles are assigned.
   - Regular meetings and communication are held to facilitate collaboration.
- Evaluation:
   - The SIG periodically assesses its progress and contributions to the project.
   - Feedback from the community is collected to improve operations.
- Maintenance:
   - The SIG continues to work on its objectives, adapting to changes in community needs.
   - New members may join, and roles may be adjusted as necessary.
- Closure:
   - If a SIG is no longer needed or fails to meet its objectives, the SIG chair, tech leader, and TSC member can propose its closure.
   - The TSC reviews and approves the closure, ensuring a smooth transition for any ongoing work.
   
## Vendor-neutrality

- The most important governance bodies in the community, such as the TSC, Maintainers, Committers, and special groups like the PSC (security team), should consist of members from different companies to ensure neutrality, with a default limit of no more than 50% from a single company.
  
- The community's infrastructure resources, including the website, Slack, and mailing lists, should be maintained collectively by community members to avoid control by a single company.

- For community communications and discussions, in the interest of transparency, information should be made as public as possible. If certain information is not suitable for broad disclosure at a specific time, it should be shared within diverse groups such as the TSC and PSC (security team) and disclosed publicly when appropriate.

- To maintain neutrality in technical architecture, the community will not adopt technical solutions that are tied to a single company's products when considering technical options. The possibility of integrating multiple providers (flexibility) should be taken into account.

## Github Project Administration

The __cubefs__ GitHub project maintainers team reflects the list of Maintainers.

## Sub-Projects

The sub-projects of CubeFS are closely related to the main project, serving as essential supplements that need to be released synchronously with the main version when necessary. Among other listed projects, some are for exploratory purposes while others are related to peripheral ecosystem products.

Current Sub-Projects:
- [CubeFS-Helm](https://github.com/cubefs/cubefs-helm):The CubeFS-Helm project helps deploy a CubeFS cluster orchestrated by Kubernetes.
- [CubeFS-CSI](https://github.com/cubefs/cubefs-csi):CubeFS Container Storage Interface (CSI) plugins.
- [CubeFS-Dashboard](https://github.com/cubefs/cubefs-dashboard):A web-admin for CubeFS.

The CubeFS organization is open to receive new sub-projects under its umbrella. To accept a project
into the __CubeFS__ organization, it has to meet the following criteria:

- Must be licensed under the terms of the Apache License v2.0
- Must be closely related to one or more areas of the CubeFS ecosystem, and CubeFS relies heavily on these projects:
  - CubeFS project artifacts (website, deployments, CI, etc)
  - External plugins
  - Other storage related topics
- Must be supported by a Maintainer not associated or affiliated with the author(s) of the sub-projects
- sub-projects can have their own repositories but follow the same governance mechanism as the main project
- Joining a Sub-Projects requires submitting an issue in the main project to obtain a vote of approval from the TSC committee. Similarly, significant actions such as project archiving also require the consent of the TSC committee

The submission process starts as a Pull Request or Issue on the
[cubefs/cubefs](https://github.com/cubefs/cubefs) repository with the required information
mentioned above. Once a project is accepted, it's considered a __sub-project under the umbrella of CubeFS__.

## Peripheral Projects

Besides the main project CubeFS and its sub projects, other projects within the repository exist as peripheral projects.

The CubeFS organization is open to receive new peripheral projects under its umbrella. To accept a project
into the __CubeFS__ organization, it has to meet the following criteria:

- Must be licensed under the terms of the Apache License v2.0
- Must be related to one or more scopes of the CubeFS ecosystem:
  - CubeFS project artifacts (website, deployments, CI, etc)
  - External plugins
  - Other storage related topics
- Peripheral-projects can have their own repositories but follow the same governance mechanism as the main project
- Joining a Peripheral Project requires submitting an issue in the main project to obtain a vote of approval from the TSC committee. Similarly, significant actions such as project archiving also require the consent of the TSC committee

## Product Security Committee(PSC)

The [Product Security Committee](https://github.com/cubefs/cubefs/blob/master/security/security-release-process.md#product-security-committee) is responsible for organizing the entire response including internal communication and external disclosure but will need help from relevant developers and release leads to successfully run this process.

## Contribute
[Details on Contributing to CubeFS](https://github.com/cubefs/cubefs/blob/master/CONTRIBUTING.md)

There is a clear definition of roles and their promotion paths.
- [Becoming a Committer](https://github.com/cubefs/cubefs/blob/master/GOVERNANCE.md#becoming-a-committer)
- [Becoming a Maintainer](https://github.com/cubefs/cubefs/blob/master/GOVERNANCE.md#becoming-a-maintainer)
- [Becoming a TSC Member](https://github.com/cubefs/cubefs/blob/master/GOVERNANCE.md#becoming-a-tsc-member)

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

- The liaison officer is responsible for daily communication with CNCF, including information updates, demand communication, community activity meetings, etc.
- The liaison officer must be a TSC member recommended internally by the TSC. The term does not have a fixed duration and can be adjusted by the TSC when its members change.
