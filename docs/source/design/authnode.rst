Authnode
=========

Internet and Intranet are insecure places where malicious hackers usually use tools to "sniff" sensitive information off of the network. Worse yet, client/server are not trusted to be honest about their identities. Therefore, CubeFS encounters some common security problems once deployed in the Network.

Security Problems
------------------

- Unauthorized parties may access the resource, such as restful API, volume information.
- Communication channel is vulnerable to `Man-in-the-middle` (`MITM`) attacks.

High Level Architecture
-----------------------

`Authnode` is the security node providing a general Authentication & Authorization framework for `CubeFS`. Besides, `Authnode` acts as a centralized key store of both symmetric and asymmetric key. `Authnode` adopts and customizes the idea of authentication from `Kerberos` which is built on top of tickets. Specifically, whenever a client node (`Master`, `Meta`, `Data` or `Client` node) accesses a service, it's firstly required to show the shared secret key for authentication in `Authnode`. If successful, `Authnode` would issue a time-limited ticket specifically for that service. With the purpose of authorization, capabilities are embedded in tickets to indicate `who can do what on what resource`.

.. image:: pic/authflow.png
   :align: center
   :scale: 50 %
   :alt: Architecture


In the context of `Authnode`, we define a node as `Client` if it is responsible to initialize a service request while `Server` or `Service` is defined as the node responding that request. In this case, any `CubeFS` nodes can be acted as either `Client` or `Server`.

The communication between `Client` and `Server` is based on `HTTPS` or `TCP` and the workflow of `Authnode` is depicted in the graph above and briefly described as follows:

Ticket Request (F1)
+++++++++++++++++++

Before any service requests, client holding a secret key *Ckey* is required to get a ticket from `Authnode` for the service (called `target service`).

- C->A: Client sends a request including Client ID (*id*) and Service ID (*sid*) indicating target service.
- C<-A: Server lookups client secret key (*CKey*) and server secret key (*Skey*) from `key store` and responds a *Ckey*-encrypted message mainly including and a session key (*sess_key*) and target service ticket (*ticket*) encrypted with secret key *Skey* of target service if verification succeeds.

After obtaining the ticket and processing some security checks with *Ckey*, client has in its possession *sess_key* and Skey{*ticket*} for future service requests.


Service Request in HTTPS (F2)
+++++++++++++++++++++++++++++

If a service request is sent via HTTPS protocol, it has the following steps:

- C->S: Client sends a request containing SKey {*ticket*}.
- C<-S: Server (1) performs message decryption and get the ticket, (2) verifies its capabilities and (3) responds the *data* which is encrypted with *sess_key* extracted from ticket.

Client uses *sess_key* to decrypt message returned from server and verify its validity.


Service Request in TCP (F3)
+++++++++++++++++++++++++++

If a service request is sent via TCP protocol, it has the following steps:

- C->S: Client sends a request containing SKey {*ticket*}.
- C<-S: Server (1) decrypts the ticket and validate its capabilities, (2) extracts *sess_key*, (3) generates a random number *s_n* and (4) responds this number encrypted with *sess_key*.
- C->S: Client decrypts the replied message and send to server another message including a randomly generated number *s_c* and *s_n* + 1, both of which are encrypted with *sess_key*.
- C<-S: Server verifies whether *s_n* has been increased by one and sends back a message including *s_c* + 1 which is encrypted by *sess_key* if the verification succeeds.
- C<->S: Client verifies whether *s_c* has been increased by one after message decryption. If successful, an authenticated communication channel has been established between client and server. Based on this channel, client and server can perform further communications.


Future Work
-----------

`Authnode` supports a general authentication and authorization which is an emergent need for `CubeFS`. There are two directions of security enhancements for `ChubaoFS` in the future.


Feature Enrichment
++++++++++++++++++

Current implementation of `Authnode` doesn't support some advanced features:

- Key rotation: Shared secret keys are hardcoded in client and server and would not changed. It increases security risks that attacks break the encryption and find the keys. Rotating keys on a regular basis would help to mitigate such risks.
- Ticket revocation: For performance considerations, ticket would be valid for a while (such as several hours). If a client unfortunately leaks its ticket, malicious parties are able to use the ticket for service request during the period of being valid. Ticket revocation mechanism can prevent such an issue by revoking it once leakage happens.
- HSM support: `Authnode` is the security bottleneck in `CubeFS`. Breaking `Authnode` means compromising the whole system since it manages the key store. Hardware Security Module or HSM provides physical safeguards for key management. Having `Authnode` protected by HSM (for example *SGX*) can mitigate the risk of `Authnode` being compromised.


End-to-End Data Encryption
++++++++++++++++++++++++++

Current implementation of `Authnode` doesn't systematically support encryption for data in transit and at rest even though we may use session key to encrypt data during communication. A more secure way to protect data is to have `End-to-End` `Data` `Encryption`. In particular, encryption keys are managed and distributed by `Authnode` and data are encrypted in client node, sent via network and stored in server. Compared with server side encryption based on existing tools (`fscrypt`, `ecryptfs` and `dm-crypt`), `End-to-End` `Data` `Encryption` has the following advantages at least:

- It mitigates data leakage once data servers (for example `Data Node`) are broken into by attackers since keys of data decoding are stored in `Authnode`.
- It provides a centralized management (rotation, revocation and generation) for encryption key.


