# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed:
- Binding the HMAC Token to a topic instead to a user id. This allows better verification of permission in the PHP backend. (by @HB9HIL)

## [0.0.4] - 2026-06-15

### Added
- WebSocket connect/disconnect log lines now include the client IP (`ip=`), taken from `X-Forwarded-For`/`X-Real-IP` if present, otherwise `RemoteAddr`. (by @HB9HIL)

## [0.0.3] - 2026-06-14

### Added
- Optional `ws_bind` / `internal_bind` config options to restrict each listener to a specific IP. Empty/omitted keeps the previous behaviour (all interfaces). (by @HB9HIL)

## [0.0.2] - 2026-06-14

### Added
- Add Tests for Wavelog Worker (by @HB9HIL)

### Fixed
- Fixed a race condition in the subscriber manager (by @HB9HIL)

## [0.0.1] - 2026-06-14

### Added
- Initial release of the Wavelog Worker.
