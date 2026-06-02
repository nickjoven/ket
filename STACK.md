# Stack coordination & release runbook

ket is the root of a small stack of repos that pin it. This file documents the
dependency arrows, the version anchors, and the ritual for cutting a new version
without leaving consumers pinned at a stale or off-`main` commit.

(Relocated from the harmonics repo: stack coordination is a substrate concern,
so it lives with the substrate. harmonics is one consumer among several.)

## Dependency arrows

```
            ┌─────────────────────────── ket ───────────────────────────┐
            │  ket-cas · ket-dag · ket-sql · ket-cdom · ket-mcp           │
            │  (CAS + Merkle DAG + Dolt SQL + canonical-doc + MCP)        │
            └──┬──────────────┬──────────────┬───────────────┬───────────┘
               │              │              │               │
            canon.d        k-stack        catbus      harmonics-seed
          (canonical-   (MCP server)   (handoff      (seeds a DAG
           doc layer,                   packer)       via canon.d)
           ket optional                                    │
           feature)                                        │
               │                                           │
               └──────────────► k-stack also pins ◄────────┘
                                canon.d
```

Everything points **at ket**. `canon.d` is a *downstream consumer* (ket is an
optional feature of canon.d, not the reverse). `k-stack` pins both ket and
canon.d. `harmonics-seed` reaches ket through canon.d. harmonics also vendors
ket as a git submodule.

## Version anchors

Two tags are the whole interface. Every consumer pins one or both.

| Repo     | Tag      | Commit    | Pinned by                                  |
|----------|----------|-----------|--------------------------------------------|
| ket      | `v0.2.0` | `3530ad5` | canon.d, k-stack, catbus, harmonics-seed   |
| canon.d  | `v0.1.0` | `0a18a32` | k-stack, harmonics-seed                     |

All consumers use the single canonical source URL
`https://github.com/nickjoven/ket.git` and pin with `tag = "v0.2.0"` — not a
rev, not a branch. There are **zero rev-pins** in the stack; keep it that way.

## Cutting a new version

Releases go **in dependency order, ket-first**. A consumer must never pin a tag
that doesn't yet exist or points off `main`.

1. **ket** — land changes on `main`. Tag the *merge commit on `main`*:
   ```sh
   git checkout main && git pull
   git tag -a v0.3.0 -m "ket 0.3.0" && git push origin v0.3.0
   git merge-base --is-ancestor v0.3.0 main && echo "on main"   # verify
   ```
2. **canon.d** — bump its ket deps to `tag = "v0.3.0"`, land on `main`, then tag
   canon.d at its merge commit and push.
3. **k-stack, catbus** — bump ket (and, for k-stack, canon.d) to the new tags.
   `cargo check` each. Land on `main`.
4. **harmonics** — bump deps in `seed/Cargo.toml`, then advance the `ket`
   submodule pointer to the new tag commit and commit.

## Rollback

Moving a *published* tag is a force-push everyone must re-fetch — avoid it;
prefer cutting `v0.3.1`. If a tag was placed on the wrong commit *before anyone
consumed it*, the recorded fix is:

```sh
git tag -f v0.3.0 <correct-commit>
git push --force origin v0.3.0
```

To roll a consumer back, re-pin its `tag = "..."` and `cargo update -p <crate>`;
for harmonics, reset the submodule pointer and commit.

## Invariants to keep

- One source URL (`…/ket.git`), tag-pinned, no revs, no branches.
- Tags live on `main`.
- Bump in dependency order: ket → canon.d → (k-stack, catbus) → harmonics.
- The submodule commit and the `tag = "vX.Y.Z"` pins agree.
