DOCUMENT "Genome Browser Sessions" AND "Genomic conversions" features in `docs/features/*.md` (two separate files)
* deployment notes
* prod paths

1. Conversions
* artifacts generated
* artifacts which are accessible from UI, and whst's the wiring which makes them accessible from the UI.

2. Sessions/tracks
* explain the process of Genome Browsers being able to load files in Genome Browsers and read them

---
what do the xenium/cmg-bioloop pollers poll?
---
xenium concurrent archivals
---
xenium legacy artifacts
* expose them from UI
---
ALL usages of cmg_id will need xenium_id incorporated
---
xenium custom wfs/steps
---
add property for cmg-bioloop to be able to call xenium API for origin path
---
document globus + slurm-conversions for karthiek
---
document UI custom behavior for legacy business objects. 
---
upload static content AND parse scripts: only in Xenium integrated wfs - watch script should run separte integrated wf for xenium
* add observer for wf subdir_wf_initiator in cmg-bioloop watcher.
---