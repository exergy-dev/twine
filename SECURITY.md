# Security policy

Twine parses untrusted input on the network path: the HTTP/1.1 parser, the
`proto` framers and AMP, and the TLS record layer around `crypto/tls`
(including kTLS key handling on Linux). Reports about any of these are
welcome.

## Reporting a vulnerability

Please do not open a public issue. Report it privately through GitHub:
<https://github.com/exergy-dev/twine/security/advisories/new> (the
"Report a vulnerability" button on the repository's Security tab).

Include the version or commit, the platform, and a reproducer if you have
one. The report stays private between you and the maintainers until a fix
is published as a GitHub security advisory.

## Supported versions

Twine is pre-1.0. Fixes go into the latest release only.

| Version | Supported |
| --- | --- |
| latest v0.x release | yes |
| older releases | no |

Vulnerabilities in Go itself or in `golang.org/x/sys` belong upstream; see
<https://go.dev/security/policy>.
