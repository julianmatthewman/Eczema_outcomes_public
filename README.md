# Code and codelists for "Cohort studies on 71 different adverse health outcomes among people with atopic eczema in UK primary care data"
[![DOI](https://zenodo.org/badge/755941643.svg)](https://zenodo.org/doi/10.5281/zenodo.10649715)

## How to run

1.  Open the R console and call `renv::restore()` to install the required R packages.
2.  Provide a `paths.R` file in the `paths/` folder (see [README](paths/README.md))
3.  Call `library(targets)` and `tar_make()` to run the pipeline.

## How to inspect targets

-   Call `tar_read(target)` to retrieve a specified target.
-   Call `tar_visnetwork(targets_only = TRUE)` to visualise the pipeline.

## Working with CPRD Aurum Define & Extract system

No real data is provided in this repository. CPRD Aurum data is provided via a 2-step process: First, the codes from the relevant codelist need to be provided to *define* a cohort of exposed individuals (the files are created as **[...]\_codes_for_define.txt**). Second, a list of patids needs to be provided to *extract* all data for these individuals (the list of patids is created as **patids_for_extract.txt**).

## Files

| File                                   | Purpose                                                                                                        |
|--------------------|----------------------------------------------------|
| [\_targets.R](_targets.R)              | Declares the [`targets`](https://docs.ropensci.org/targets) pipeline.                                          |
| [R/](R/)                               | Contains R scripts with functions to be used in the pipeline.                                                  |
| [codelists/](codelists/)               | Contains all codelists (and some that were not used).                                                          |
| [dummy_data/](dummy_data/)             | When the pipeline is run, dummy data will be placed here.                                                      |
| [output/](output/)                     | When the pipeline is run, outputs will be placed here.                                                         |
| [sensitive_output/](sensitive_output/) | When the pipeline is run, sensitive outputs will be placed here (lists for CPRD Aurum define and extract).     |
| [paths/](paths/)                       | To run the pipeline, the `paths.R` file needs to be placed here (see [README](paths/README.md)).               |
| [renv.lock](renv.lock)                 | The [`renv`](https://rstudio.github.io/renv/articles/renv.html) lock file that specifies all package versions. |
