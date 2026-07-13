# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.1] - 2026-07-13

### Added
- internal/status now returns a list of active topics (with at least one subscriber) in addition to the list of all registered topics. (by @HB9HIL)

### Changed
- internal/status now only returns the topic lists when requested via `?topics=1`. This saves bandwidth on the debug page, as the topic lists can be very large. (by @HB9HIL)

## [0.2.0] - 2026-07-08

### Added
- Added websocket status handler so healthchecks can be performed on the websocket listener. (by @HB9HIL)

### Changed
- Strip the 'v' from the injected version in the docker image. (by @HB9HIL)

## [0.1.2] - 2026-07-08

### Fixed
- Fixed version injection in docker image. (by @HB9HIL)

## [0.1.1] - 2026-07-04

### Fixed
- Fixed memory leak in MemRegistry (single mode) where topics never go cleaned up. RedisRegistry (cluster mode) already had the 24h TTL. Value can be configured if necessary via the `topic_ttl` config option. (by @HB9HIL)

## [0.1.0] - 2026-06-27

### Changed
- The `internal_bind` config option now defaults to `127.0.0.1` instead of `0.0.0.0`. This is a breaking change for docker users, as the Wavelog Worker will no longer be able to reach the internal API. If you run the Wavelog Worker in Docker you need to set `internal_bind` to `0.0.0.0` to allow Wavelog to reach the internal API. The Docker network isolation protects it. (by @HB9HIL)
- Sanitize the client ip to prevent log injection. (by @HB9HIL)
- Use SCAN instead of KEYS to list topics in Redis, preventing a potential DoS if there are many topics. (by @HB9HIL)

## [0.0.6] - 2026-06-16

### Fixed
- Fixed a panic (send on closed channel) when a client disconnected while an event was being published to it. The WebSocket client now guards its send channel with a `closed` flag and unsubscribes before closing. (by @int2001)
- WebSocket connections now enforce a 4 KiB inbound frame limit (`SetReadLimit`), preventing a single client from exhausting memory with an oversized frame. (by @int2001)

## [0.0.5] - 2026-06-16

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
