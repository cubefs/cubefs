# Authentication

As we all know, both the Internet and intranet are insecure places, and hackers can often use tools to sniff out a lot of sensitive information on the network. Worse still, because it is impossible to confirm whether the other party has honestly expressed its identity, there is no way to establish a trusted connection between the client and the server. Therefore, if there is no authentication mechanism, CubeFS will have some common security issues once deployed on the network.

## Security Issues

- Unauthorized nodes may access sensitive information, such as `restful API`, `volume` information, etc.
- The communication channel may be subject to `Man-in-the-middle (MITM)` attacks.

## Architecture

`Authnode` is a secure node that provides a general authentication and authorization framework for CubeFS. In addition, `Authnode` also serves as a centralized key storage for symmetric and asymmetric keys. `Authnode` adopts and customizes the ticket-based `Kerberos` authentication idea. Specifically, when a client node (`Master, MetaNode, DataNode`, or client node) accesses a service, it needs to present a shared key for authentication in `Authnode`. If the authentication is successful, `Authnode` will issue a time-limited ticket specifically for that service. For authorization purposes, the functionality is embedded in the ticket to indicate who can do what on what resources.

![Architecture](./pic/authflow.png)

In the context of `Authnode`, we define the node responsible for initiating a service request as the `Client`, and the node that responds to the request as the `Server`. Therefore, any CubeFS node can act as a `Client` or `Server`.

Communication between `Client` and `Server` is done through `HTTPS` or `TCP`. The workflow of `Authnode` is shown in the above figure and is briefly described as follows:

### Credential Request (F1)

Before any service request, the client holding the key `Ckey` needs to obtain the authentication information of the service (`target service`) from `Authnode`.

- C->A: The client sends an authentication request, which includes a client ID (`id`) representing the client and a service ID (`sid`) representing the target service.
- C<-A: The server looks up the client key (`CKey`) and the server key (`SKey`) from its own key store. If the authentication is successful, the server responds to the client with a message encrypted by `CKey`. The message contains a session key (`sess_key`) and the credentials of the target service.

After obtaining the credentials and performing some security checks using `Ckey`, the client has `sess_key` and `Skey{*ticket*}` for future service requests.

### Service request in HTTPS (F2)

If a service request is sent via the HTTPS protocol, it will follow the following steps:

- C->S: The client sends a request containing SKey {ticket}.
- C<-S: The server performs the following operations:
    - Decrypts the message and obtains the credentials
    - Verifies the function
    - Uses the sess_key obtained from the credentials to encrypt the data and return it to the client.

The client uses sess_key to decrypt the message returned from the server and verify its validity.

### Service Request in TCP (F3)

If a service request is sent via the TCP protocol, it will follow the following steps:

- C->S: The client sends a request containing SKey {ticket}.
- C<-S: The server
    - Decrypts the credentials and verifies their function
    - Extracts `sess_key`
    - Generates a random number `s_n`
    - Responds to the client with this number encrypted with `sess_key`.
- C->S:
  The client decrypts the message and sends another message to the server, which includes the randomly generated numbers `s_c` and `s_n+1`, both encrypted with `sess_key`.
- C<-S:
  The server verifies if `s_n` has increased by 1. If the verification is successful, the server will send a message containing `s_c+1`, encrypted with `sess_key`.
- C<->S:
  The client verifies if `s_c` has increased by one after decrypting the message. If successful, an authenticated communication channel has been established between the client and server. Based on this channel, the client and server can perform further communication.

## Future Work

`Authnode` supports the much-needed general authentication and authorization for CubeFS. There are two directions for future security enhancements in `CubeFS`:

### Feature-rich

The current implementation of `Authnode` does not support some advanced features:

- Key rotation: Shared keys are hard-coded in the client and server and do not change. This increases security risks as an attack can break the encryption and find the key. Regularly rotating keys can help mitigate such risks.
- Credential revocation: For performance reasons, credentials are valid for a certain period of time (e.g. a few hours). If a client unfortunately leaks its ticket, a malicious party can use the ticket for service requests within the validity period. A credential revocation mechanism can prevent such issues by revoking credentials when a leak occurs.
- HSM support: `Authnode` is a security bottleneck in CubeFS. Breaking `Authnode` means breaking the entire system as it manages key storage. Hardware security modules (HSMs) provide physical protection for key management. Protecting `Authnode` with an HSM (e.g. SGX) can reduce the risk of `Authnode` being compromised.

### End-to-end Data Encryption

The current implementation of `Authnode` does not systematically support encryption of data in transit and at rest, even though we can use session keys to encrypt data during communication. A more secure way to protect data is to use `end-to-end data encryption`. Specifically, the encryption keys are managed and distributed by `Authnode`, data is encrypted at the client node, sent over the network, and stored on the server. Compared to server-side encryption based on existing tools (`fscrypt`, `ecryptfs`, and `dm-crypt`), `end-to-end data encryption` has at least the following advantages:

- Since the data decryption key is stored in `Authnode`, it can reduce data leaks once the data server (e.g. `data Node`) is compromised.
- It provides centralized management (rotation, revocation, and generation) of encryption keys.