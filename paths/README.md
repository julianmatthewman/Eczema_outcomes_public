# `paths/`

Here, in a file named `paths.R` define paths to the data.
If you want to run the pipeline with dummy data, set the following paths:

```
path_extract <- "dummy_data/extract/"
path_denominator <- "dummy_data/denominator/AcceptablePats.parquet"
path_define <- "dummy_data/define/"
path_hes_eligibility <- "dummy_data/linkage/Aurum_enhanced_eligibility_January_2022.dta"
```

In addition, you need to provide the path to a folder containing the CPRD Aurum codebrowsers in Stata .dta format: CPRDAurumMedical.dta and CPRDAurumProduct.dta
```
path_codebrowsers <- "path/to/the/codebrowsers/"
```