# Changelog

# v0.1.16

- Pulsar token authentication from `PULSAR_TOKEN_FILE` or `PULSAR_TOKEN`, with
  `pulsar_authentication()` for components that build their own client.
- `PULSAR_DEBUG_TOPIC`, `PULSAR_TAKEOVER_ENABLED` and `PULSAR_DEAD_LETTER_TOPIC` to move the
  Runner's debug and dead letter topics out of `public/default`, or to skip the takeover
  subscription.
- The Runner routes messages by fully qualified topic name, so Messagers can be registered
  under fully qualified topics, and messages from partitioned topics are routed.

# v0.1.12 (2025-06-10)

- Updated to use one Pulsar listener per-thread, without which multi-threading support
  was not used.

# v0.1.0 (2024-05-21)

- Added some pulsar support
