#' Phenotype for moderate atopic eczema severity
#' @description 
#' Moderate eczema: The first date that someone is prescribed potent topical steroids or calcineurin inhibitors
#' Severe eczema: The first date that someone is referred to a dermatologist, prescribed a systemic drug (azathioprine, cyclosporine, methotrexate, or mycophenolate mofetil), or had a record for phototherapy
#' Individuals can progress from mild to moderate eczema at the first record, suggesting moderate disease, and from mild or moderate eczema to severe eczema at the first record, suggesting severe disease.
#' @references 
#' Lowe KE, Mansfield KE, Delmestri A, Smeeth L, Roberts A, Abuabara K, et al. Atopic eczema and fracture risk in adults: A population-based cohort study. Journal of Allergy and Clinical Immunology. 2020 Feb;145(2):563-571.e8. 

alg_eczema_severity <- function(exposed_obs_defined, exposed_drug_defined, codelists_for_define) {
	
	pot_top_cs_prodcodes <- codelists_for_define$full[[which(codelists_for_define$name=="topical_glucocorticoids")]] |> 
		filter(potency %in% c("potent", "very potent")) |> 
		pull(ProdCodeId)
	
	eventdata_eczema <- exposed_obs_defined |> 
		collapse::fsubset(medcodeid %in% codelists_for_define$codes[[which(codelists_for_define$name=="eczema")]]) |> 
		rename(eventdate=obsdate)
	eventdata_phototherapy <- exposed_obs_defined |> 
		collapse::fsubset(medcodeid %in% codelists_for_define$codes[[which(codelists_for_define$name=="phototherapy")]]) |> 
		rename(eventdate=obsdate)
	eventdata_top_cs <- exposed_drug_defined |> 
		collapse::fsubset(prodcodeid %in% codelists_for_define$codes[[which(codelists_for_define$name=="topical_glucocorticoids")]]) |> 
		rename(eventdate=issuedate)
	eventdata_emollients <- exposed_drug_defined |> 
		collapse::fsubset(prodcodeid %in% codelists_for_define$codes[[which(codelists_for_define$name=="emollients")]]) |> 
		rename(eventdate=issuedate)
	eventdata_oral_glucocorticoids <- exposed_drug_defined |> 
		collapse::fsubset(prodcodeid %in% codelists_for_define$codes[[which(codelists_for_define$name=="oral_glucocorticoids")]]) |> 
		rename(eventdate=issuedate)
	eventdata_pot_top_cs <- exposed_drug_defined |> 
		collapse::fsubset(prodcodeid %in% codelists_for_define$codes[[which(codelists_for_define$name=="topical_glucocorticoids")]]) |> 
		filter(prodcodeid %in% pot_top_cs_prodcodes) |> 
		rename(eventdate=issuedate)
	eventdata_top_cn_inh <- exposed_drug_defined |> 
		collapse::fsubset(prodcodeid %in% codelists_for_define$codes[[which(codelists_for_define$name=="topical_calcineurin_inhibitors")]]) |> 
		rename(eventdate=issuedate)
	eventdata_sys_imspr <- exposed_drug_defined |> 
		collapse::fsubset(prodcodeid %in% codelists_for_define$codes[[which(codelists_for_define$name=="systemic_immunosupressants")]]) |> 
		rename(eventdate=issuedate)
	
	
	
	
	eczema_obs <- eventdata_eczema	|> 
		filter(!duplicated(patid)) |> 
		select(patid, eczema_date=eventdate)
	
	eventdata_eczemaRx <- bind_rows(eventdata_top_cs,
																	eventdata_emollients, 
																	eventdata_oral_glucocorticoids, 
																	eventdata_pot_top_cs,
																	eventdata_top_cn_inh,
																	eventdata_phototherapy,
																	eventdata_sys_imspr)
	
	eczemaRx_obs <- eventdata_eczemaRx |> 
		arrange(eventdate) |> 
		group_by(patid) |> 
		filter(!duplicated(eventdate)) |> 
		slice(2) |> 
		ungroup() |>
		select(patid, second_prescription_date=eventdate)
	
	mild <- eczema_obs |> 
		left_join(eczemaRx_obs, by="patid") |>  
		mutate(exposed_date=pmax(eczema_date, second_prescription_date)) |> 
		filter(!is.na(exposed_date)) |> 
		mutate(eventdate=exposed_date) |> 
		select(patid, eventdate) |> 
		mutate(eczema_severity=1)
	
	moderate <- bind_rows(eventdata_pot_top_cs, eventdata_top_cn_inh) |> 
		group_by(patid) |> 
		arrange(eventdate) |> 
		slice(1) |> 
		ungroup() |> 
		select(patid, eventdate) |> 
		mutate(eczema_severity=2)
	
	severe <- bind_rows(eventdata_phototherapy, eventdata_sys_imspr) |> 
		group_by(patid) |> 
		arrange(eventdate) |> 
		slice(1) |> 
		ungroup() |> 
		select(patid, eventdate) |> 
		mutate(eczema_severity=3)
	
	rbind(mild, moderate, severe) |> 
		arrange(patid, eventdate) |> 
		group_by(patid) |> 
		slice(if(any(eczema_severity==3)) 1:which(eczema_severity==3) else 1) |> 
		ungroup()
}
