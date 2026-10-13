# Security policy

## Supported versions

Security fixes are made on `main` and published as the
`levonghukas/orabbit:latest-master` and `levonghukas/orabbit:latest-worker`
images. Older images are not patched, so keep deployments up to date with
`docker compose pull`.

## Reporting a vulnerability

**Please do not report security problems in public issues, discussions or pull
requests.**

Report them privately through GitHub:
[**Report a vulnerability**](https://github.com/LevonGhukas/O_Rabbit/security/advisories/new)
(Security tab → Advisories → Report a vulnerability).

Include as much as you can:

- the affected component (master, worker, CLI, API route, connector)
- steps to reproduce, or a proof of concept
- the impact you expect, for example credential exposure or unauthorized access
- the version or image digest you tested

You will get an acknowledgement as soon as possible. We will keep you updated
while the fix is prepared and credit you in the advisory unless you prefer
otherwise.

## Hardening guidance

The [Security model](README.md#security-model) section of the README explains
how secrets are encrypted, how workers are authenticated, and which settings
must never be combined on a routable network.
