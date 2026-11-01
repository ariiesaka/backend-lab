# backend-lab

My personal documentation of backend-related implementations: ideas I am thinking about or want to build, written down as working code. It exists for my own purposes and is written with the help of Claude Opus 5.5.

Implementations can be in any language and are grouped by language.

## Implementations

| Topic | Language | Summary |
| --- | --- | --- |
| [Feature control](go/featurecontrol) | Go | Two ways to serve feature flags without calling the database on every read: an on-demand cache with a configurable TTL, and a snapshot of every flag refreshed on a timer. |
