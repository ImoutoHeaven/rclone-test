---
title: "Myrient Remote"
description: "Read-only backend for Myrient files index"
---

Use `myrient:` to list and copy from `https://myrient.erista.me/files/`.

This backend is read-only.

Example:

```console
rclone lsd myrient:
rclone copy myrient:"Eggman's Arcade Repository/Adrenaline Amusements" /data/myrient
```
