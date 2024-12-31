# ===== laps2() =====
# Calculate LAPS2 scores from a provided dataframe "data" using CLIF tables (see args). The LAPS2 scores are joined to the original "data" dataframe and returned to user.

# Inputs: "data": Dataframe of interest - laps2 score column will be added to this dataframe. CLIF tables required to calculate LAPS2 (see "args(laps2)")
# Output: "data": Input dataframe of interest with an additional "laps2" column

# NEED TO FIX!! NEED TO MAKE SURE INPUTS ARE CONSISTENT - NEED TO ADD CODE TO VALIDATE THE COLUMN DATATYPES AND TIMEZONE FOR THE INPUTS FOR THIS FUNCTION!!

laps2_date <- function(data=NULL, clif_adt=NULL, clif_labs=NULL, clif_hospitalization=NULL, clif_patient=NULL, clif_vitals=NULL, clif_gcs=NULL) {
    # Check that required dataframes exist. Use global dataframe is not explicitly provided
    if (is.null(data)) {
        if (exists("data", envir = .GlobalEnv)) {
        data <- get("data", envir = .GlobalEnv)
        print('No |data| input provided. Global |data| dataframe is being used')
        } else {
        stop("|data| not provided and no global |data| found.")
        }
    }
    if (is.null(clif_adt)) {
        if (exists("clif_adt", envir = .GlobalEnv)) {
        clif_adt <- get("clif_adt", envir = .GlobalEnv)
        print('No |clif_adt| input provided. Global |clif_adt| dataframe is being used')
        } else {
        stop("|clif_adt| not provided and no global |clif_adt| found.")
        }
    }
    if (is.null(clif_labs)) {
        if (exists("clif_labs", envir = .GlobalEnv)) {
        clif_labs <- get("clif_labs", envir = .GlobalEnv)
        print('No |clif_labs| input provided. Global |clif_labs| dataframe is being used')
        } else {
        stop("|clif_labs| not provided and no global |clif_labs| found.")
        }
    }
    if (is.null(clif_hospitalization)) {
        if (exists("clif_hospitalization", envir = .GlobalEnv)) {
        clif_hospitalization <- get("clif_hospitalization", envir = .GlobalEnv)
        print('No |clif_hospitalization| input provided. Global |clif_hospitalization| dataframe is being used')
        } else {
        stop("|clif_hospitalization| not provided and no global |clif_hospitalization| found.")
        }
    }
    if (is.null(clif_patient)) {
        if (exists("clif_patient", envir = .GlobalEnv)) {
        clif_patient <- get("clif_patient", envir = .GlobalEnv)
        print('No |clif_patient| input provided. Global |clif_patient| dataframe is being used')
        } else {
        stop("|clif_patient| not provided and no global |clif_patient| found.")
        }
    }
    if (is.null(clif_vitals)) {
        if (exists("clif_vitals", envir = .GlobalEnv)) {
        clif_vitals <- get("clif_vitals", envir = .GlobalEnv)
        print('No |clif_vitals| input provided. Global |clif_vitals| dataframe is being used')
        } else {
        stop("|clif_vitals| not provided and no global |clif_vitals| found.")
        }
    }
    if (is.null(clif_gcs)) {
        if (exists("clif_gcs", envir = .GlobalEnv)) {
        clif_gcs <- get("clif_gcs", envir = .GlobalEnv)
        print('No |clif_gcs| input provided. Global |clif_gcs| dataframe is being used')
        } else {
        stop("|clif_gcs| not provided and no global |clif_gcs| found.")
        }
    }

# Make sure recorded_date is a date not integer - chicago hardcoded but could change that
##add more?
# Get user's timezone to use in any as.Date() functions!! Important to get correct dates!!
user_timezone <- Sys.timezone()
#data$recorded_date_timestamp <- data$recorded_date_timestamp |> mutate(recorded_date_timestamp = as.POSIXct(recorded_date_timestamp, tz = user_timezone))
data$recorded_date <- as.Date(data$recorded_date_timestamp, tz = user_timezone)


# Convert hospitalizations_joined_id to strings
data$hospitalizations_joined_id <- as.character(data$hospitalizations_joined_id)
clif_adt$hospitalizations_joined_id <- as.character(clif_adt$hospitalizations_joined_id)
clif_labs$hospitalizations_joined_id <- as.character(clif_labs$hospitalizations_joined_id)
clif_hospitalization$hospitalizations_joined_id <- as.character(clif_hospitalization$hospitalizations_joined_id)
clif_patient$hospitalizations_joined_id <- as.character(clif_patient$encounter_id)
clif_vitals$hospitalizations_joined_id <- as.character(clif_vitals$hospitalizations_joined_id)
clif_gcs$hospitalizations_joined_id <- as.character(clif_gcs$hospitalizations_joined_id)
print('String conversion done')

# Right join to only keep data corresponding to the main regression data
clif_adt <- clif_adt |>
  right_join(data |> dplyr::select(hospitalizations_joined_id) |> dplyr::distinct())
clif_labs <- clif_labs |>
  right_join(data |> dplyr::select(hospitalizations_joined_id) |> dplyr::distinct())
clif_hospitalization <- clif_hospitalization |>
  right_join(data |> dplyr::select(hospitalizations_joined_id) |> dplyr::distinct())
clif_patient <- clif_patient |>
  right_join(data |> dplyr::select(hospitalizations_joined_id) |> dplyr::distinct())
clif_vitals <- clif_vitals |>
  right_join(data |> dplyr::select(hospitalizations_joined_id) |> dplyr::distinct())
clif_gcs <- clif_gcs |>
  right_join(data |> dplyr::select(hospitalizations_joined_id) |> dplyr::distinct())


clif_gcs <- clif_gcs |> 
  filter(assessment_category == "gcs_total") |> 
      # Check if Variables match!!
      dplyr::select(
        hospitalizations_joined_id, hospitalization_id,
        recorded_dttm,
        gcs_total = numerical_value
        )


#CE changes - commented out ni_tic() and ni_toc(), lactate to lactic_acid
# Laps2 Score
## encounter data

### change to hospital_icu
clif_adt_hospital <-  clif_adt |>
  mutate(location_category = factor(location_category,
                                    levels = (c("icu", "stepdown", "ward", "ed", "procedural", "other")),                                    ordered = TRUE)) |> 
  group_by(hospitalizations_joined_id) |> 
  arrange(hospitalizations_joined_id, location_category, in_dttm) |> 
  
  # first one will be ICU
  slice_head(n=1) |> 
  ungroup() |> 
  dplyr::select(hospitalizations_joined_id,
         hospital = hospital_id)


clif_adt_admit <-  clif_adt |>
  group_by(hospitalizations_joined_id) |> 
  arrange(hospitalizations_joined_id, in_dttm) |> 
  slice_head(n=1) |> 
  ungroup() |> 
  dplyr::select(hospitalizations_joined_id,
         first_location_category = location_category)
  

# fixing clif_labs to recorded_dttm
if ("lab_collect_dttm" %in% colnames(clif_labs)) {
  clif_labs <- clif_labs |> rename(recorded_dttm = lab_collect_dttm)
}

df_encounter_laps2_temp1 <- clif_hospitalization |> 
  left_join(clif_patient) |> 
  left_join(clif_adt_hospital) |> 
  left_join(clif_adt_admit) |> 
  
  # drop this so we can call distinct and get rid of duplicates
  dplyr::select(-hospitalization_id) |> 
  distinct() |> 
  
  # make dead/hospice column
  mutate(
    death_or_hospice_01 = fcase(
      discharge_category %in% 
        c("hospice", "dead", "expired", "died")       , 1,
      default                                         = 0
    ),
    
    death_or_hospice_01       = as.factor(death_or_hospice_01),
    
    # make 24 hours after admission variable (to collect data from first 24 hours)
    dt_24hours_after_admit    = admission_dttm + hours(24),
    
    # ED binary variable
    ed_admit_01               = fifelse(first_location_category == "ed", 1, 0, 0),
          # I think we used categorical in Rachel Kohn paper... Pat uses ED y/n
          # admission source OR you can use ED variable
    # Admit source variable (use one or the other)
    # admission_type_category      = as.factor(admission_type_category),  don't have this yet
    
    # sex_category
    female_01                 = fifelse(str_detect(sex_category, "female"), 1, 0, 0),
    
  )


## Anion Gap with 1 hour tolerance

if(any(clif_labs$lab_category == "anion_gap", na.rm = TRUE)){
  
  print("you already have anion gap")
  message("we are going to remove anion_gap and recreate it for you")
  message("this way... all our analyses are the same")
  
  clif_labs <- clif_labs |> 
    filter(lab_category != "anion_gap")

    print("Anion gap removed... please proceed!!")
  
} else {
  
  print("no anion_gap found... we will make one for you!!")
  
  
}


if(any(clif_labs$lab_category == "troponin_t")){
  
  print("you already have trop_t")
  message("we are going to change to trop_i levels")
  
  clif_labs <- clif_labs |> 
    
    mutate(lab_value_numeric = fcase(
      lab_category == "troponin_t",
      lab_value_numeric / 1000,
      default = lab_value_numeric
    )) |> 
    
    mutate(lab_category = fcase(
      lab_category == "troponin_t",
      "troponin_i",
      rep_len(TRUE, length(lab_category)), lab_category
    )) 
    
    print("Troponin fixed ... please proceed!!")
  
} else {
  
  print("no need to fix troponin !!")
  
}

# Set a time tolerance, e.g., 1 hour
time_tolerance <- hours(1)

# Create recorded_date from recorded_dttm
clif_labs <- clif_labs %>%
  mutate(recorded_date = as.Date(recorded_dttm, tz = user_timezone))

# Separate the lab values for sodium, bicarbonate, and chloride, and create recorded_date
sodium_df <- clif_labs %>%
  filter(lab_category == "sodium") %>%
  dplyr::select(hospitalizations_joined_id, recorded_dttm, recorded_date, sodium = lab_value_numeric,
         lab_order_dttm, lab_result_dttm)

bicarbonate_df <- clif_labs %>%
  filter(lab_category == "bicarbonate") %>%
  dplyr::select(hospitalizations_joined_id, recorded_dttm, recorded_date, bicarbonate = lab_value_numeric)

chloride_df <- clif_labs %>%
  filter(lab_category == "chloride") %>%
  dplyr::select(hospitalizations_joined_id, recorded_dttm, recorded_date, chloride = lab_value_numeric)

# Perform a join by hospitalizations_joined_id and recorded_date to limit the join size
anion_gap_df <- sodium_df %>%
  full_join(bicarbonate_df, by = c("hospitalizations_joined_id", "recorded_date")) %>%
  full_join(chloride_df, by = c("hospitalizations_joined_id", "recorded_date")) %>%
  
  # Calculate the time differences between recorded_dttm values
  mutate(time_diff_sodium_bicarbonate = abs(difftime(recorded_dttm.x, recorded_dttm.y, units = "mins")),
         time_diff_sodium_chloride = abs(difftime(recorded_dttm.x, recorded_dttm, units = "mins")),
         
         # Apply the time tolerance (1 hour)
         within_tolerance_bicarbonate = time_diff_sodium_bicarbonate <= time_tolerance,
         within_tolerance_chloride = time_diff_sodium_chloride <= time_tolerance) %>%
  
  # Filter to retain rows where the time differences are within the tolerance
  filter(within_tolerance_bicarbonate & within_tolerance_chloride) %>%
  
  # Calculate the anion gap: sodium - (bicarbonate + chloride)
  mutate(anion_gap = sodium - (bicarbonate + chloride)) %>%
  
  # Select the relevant columns
  dplyr::select(hospitalizations_joined_id, recorded_dttm.x, anion_gap, lab_order_dttm, lab_result_dttm, recorded_date)


# Rename recorded_dttm.x to recorded_dttm for consistency
anion_gap_df <- anion_gap_df %>%
  mutate(reference_unit = "meq/l") |> 
  mutate(lab_name = "anion gap") |> 
  mutate(lab_category = "anion_gap") |>
  rename(lab_value_numeric = anion_gap) |> 
  rename(recorded_dttm = recorded_dttm.x)


clif_labs <- clif_labs |> 
              bind_rows(anion_gap_df)




## prelaps
############
# Pre-Laps... want the most recent within 24 hours of admission
############



df_pre_laps2_prep1 <- 
  df_encounter_laps2_temp1 |> 
  left_join(clif_labs |> 
              filter(lab_category %in% c("anion_gap",
                                         "bicarbonate", # FROM BMP
                                         "bun",
                                         "creatinine",
                                         "sodium")),
            
            # join only if between the first 24 hours
            by = join_by(hospitalizations_joined_id, 
                         dt_24hours_after_admit >= recorded_dttm) 
  ) |> 
  # Min/Max
  #     bun            max
  #     creatinine     min
  #     bun/cr         max
  #     bun/cr         most recent (prelaps)
  #     anion_gap      max
  #     bicarbonate min
  #     ag/bicarb      most recent (prelaps)
  #     sodium         min
  #     sodium         most recent (prelaps)
       
  group_by(hospitalizations_joined_id, recorded_dttm, lab_category) |> 
  # using obs lets you get away with duplicates when going wider and can fix later!
  mutate(obs = row_number()) |> 
  ungroup() |> 
  pivot_wider(
    id_cols = c(hospitalizations_joined_id, recorded_dttm, obs),
    names_from = lab_category,
    values_from = lab_value_numeric,
    # names_prefix = "lab_wide_",
    # values_fill = NA
  ) |>
  
  # Quick min/max for duplicates at the SAME time
  dplyr::select(-obs) |> 
  distinct() 

# find the duplicates by recorded_dttm
df_pre_laps2_prep2 <- df_pre_laps2_prep1 |> 
  arrange(hospitalizations_joined_id, recorded_dttm) |> 
  group_by(hospitalizations_joined_id, recorded_dttm) |>
  # mutate(n = n()) |> 
  # filter(n > 1) |> 
  summarise(
    bun               = fmax(bun, na.rm = TRUE),
    creatinine        = fmin(creatinine, na.rm = TRUE),
    bicarbonate       = fmin(bicarbonate, na.rm = TRUE),
    anion_gap         = fmax(anion_gap, na.rm = TRUE),
    sodium            = fmin(sodium, na.rm = TRUE),

    .groups = "drop"
  ) |> 
  
  # replace inf with NAs
  mutate(across(where(is.numeric) & !c(hospitalizations_joined_id), ~ na_if(.x, is.infinite(.x)))) |> 
  
  # get rid of duplicate rows
  distinct() |> 

# df_pre_laps2_prep2 <- df_pre_laps2_prep1 |> 
#   anti_join(df_pre_laps2_duplicates |> 
#               dplyr::select(hospitalizations_joined_id, recorded_dttm),
#             join_by(hospitalizations_joined_id, recorded_dttm)) |> 
#   bind_rows(df_pre_laps2_duplicates) |> 
  
  # order appropriately for when getting last labs for "most recent"
  arrange(hospitalizations_joined_id, recorded_dttm) |> 
  
  # carry forward
  group_by(hospitalizations_joined_id) |> 
  
  # fill in ... its only the first 24 hours so its ok
  fill(c(bun,
         creatinine,
         bicarbonate,
         anion_gap,
         sodium), 
       .direction = "downup") |>
  
  # ? time shift if needed
  
  # get the most recent lab (aka last) based on recorded_dttm
    mutate(
      bun_recent                = flast(bun, na.rm = TRUE),
      creatinine_recent         = flast(creatinine, na.rm = TRUE),
      bicarbonate_recent        = flast(bicarbonate, na.rm = TRUE),
      anion_gap_recent          = flast(anion_gap, na.rm = TRUE),
      sodium_recent             = flast(sodium, na.rm = TRUE)
           ) |> 
  ungroup() |> 
  dplyr::select(hospitalizations_joined_id, ends_with("recent")) |> 

  # final pre-laps labs needed
  mutate(
    # make ratios
    sodium              = sodium_recent,
    bun_cr_ratio        = bun_recent/creatinine_recent,
    ag_hco3_ratio       = (anion_gap_recent/bicarbonate_recent)*1000
  ) |> 
  dplyr::select(-ends_with("recent")) |> 
  
  # replace inf with NAs (sometimes it happens when divided by zero)
  mutate(across(where(is.numeric) & !c(hospitalizations_joined_id), ~ na_if(.x, is.infinite(.x)))) |> 
  
  distinct()
  
  

# replacing all the admission values that are missing with normal values
        # replace BUNCREAT_RECENT_24=8 if BUNCREAT_RECENT_24==. // 4724
        # replace NA_RECENT_24=135 if NA_RECENT_24==. // 4270
        # replace AGHCO3_RECENT_24=200 if AGHCO3_RECENT_24==. // 5103
        
 df_pre_laps_merging <- df_pre_laps2_prep2 |> 
   left_join(
     df_encounter_laps2_temp1 |> 
       dplyr::select(
         hospitalizations_joined_id,
         admission_dttm,
         age = age_at_admission,
         female_01,
         ed_admit_01,
         # admission_type_category,
         death_or_hospice_01
       ) |> 
       distinct(),
     join_by(hospitalizations_joined_id)
   ) |> 
   mutate(
    age_cat = case_when(
      age >= 85 ~ 5,
      age >= 75 ~ 4,
      age >= 65 ~ 3,
      age >= 40 ~ 2,
      TRUE ~ 1
    )
  ) |> 
   mutate(
      # impute to normal (8)
    bun_cr_ratio = fifelse(is.na(bun_cr_ratio), 8, bun_cr_ratio),
     
    buncreat_cat = case_when(
      bun_cr_ratio        >= 24 ~ 4,
      bun_cr_ratio        >= 16 ~ 3,
      bun_cr_ratio        < 8   ~ 2,
      TRUE                      ~ 1
    )
  ) |> 
     mutate(
      # impute to normal (135)
    sodium = fifelse(is.na(sodium), 135, sodium),
    na_cat = case_when(
      sodium            >= 155  ~ 7,
      sodium            >= 149  ~ 6,
      sodium            >= 146  ~ 5,
      sodium            >= 132  ~ 4,
      sodium            >= 129  ~ 3,
      sodium            < 129   ~ 2,
      TRUE                      ~ 1
    )
  ) |> 
     mutate(
      # impute to normal (200)
    ag_hco3_ratio = fifelse(is.na(ag_hco3_ratio), 200, ag_hco3_ratio),
    aghco3_cat = case_when(
      ag_hco3_ratio        >= 600  ~ 4,
      ag_hco3_ratio        >= 400  ~ 3,
      ag_hco3_ratio        < 200   ~ 2,
      TRUE                         ~ 1,
    )
  )
 

 # logistic regression to obtain p(death/hospice)
 lr_model <-
  glm(
    death_or_hospice_01 ~
      age +
      female_01 +
      ed_admit_01 +
      bun_cr_ratio +
      sodium +
      ag_hco3_ratio,
    family = "binomial",
    data   = df_pre_laps_merging
  )

tidy(lr_model)
pR2(lr_model)["McFadden"] 


df_pre_laps_final  <- df_pre_laps_merging |> 
  mutate(p_death = predict(lr_model, df_pre_laps_merging, type = "response")) |> 
  mutate(
    high_risk = case_when(
      p_death >= 0.06 ~ 1,
      TRUE ~ 0
    )) %>%
  dplyr::select(hospitalizations_joined_id, p_death, high_risk)


#The above has like 58% high risk... that seems too high
#See testing below but ultimately I think its ok to use the predictions as above and not the published ones below.  
#R2 is 17 (using GLM) vs 11 (using numbers from Escobar study) when doing glm(death ~ p_death) in isolation.  Despite having a high number of high risk (58% with glm vs 10% with escobar numbers), i think it will work out ok


## laps2 labs ALL
##~~~~~~~~~~~~~~~~~~~~~
# LAPS2 time
##~~~~~~~~~~~~~~~~~~~~~
 
df_laps2_prep1 <- 
  df_encounter_laps2_temp1 |> 
  dplyr::select(hospitalizations_joined_id, admission_dttm, discharge_dttm) |> 
  distinct() |> 
  # slice_head(n=1000) |> 
  left_join(clif_labs |> 
              filter(lab_category %in% c("albumin",
                                     "anion_gap",
                                     "bilirubin_total",
                                     "bun",
                                     "bicarbonate",
                                     "creatinine",
                                     "glucose_serum",
                                     # "hematocrit",
                                     "hemoglobin",
                                     # "lactate", - CE change
                                     "lactic_acid",
                                     "pco2_arterial",
                                     "po2_arterial",
                                     "ph_arterial",
                                     "platelet_count",
                                     "so2_arterial",
                                     "sodium",
                                     "troponin_i",
                                     "wbc")) |> 
              dplyr::select(-lab_order_dttm),
            
            # join only if between the first 24 hours
            by = join_by(hospitalizations_joined_id)) |> 
  # making all hemoglobin into hematocrits
  mutate(lab_value_numeric = fifelse(lab_category == "hemoglobin", lab_value_numeric*3, lab_value_numeric)) |> 
  mutate(lab_category = fifelse(lab_category == "hemoglobin", "hematocrit", lab_category)) |> 
  mutate(lab_category = fifelse(lab_category == "lactate", "lactic_acid", lab_category)) |> 

    # using obs lets you get away with duplicates when going wider and can fix later!
  group_by(hospitalizations_joined_id, recorded_dttm, lab_category) |> 
  mutate(obs = row_number()) |> 
  ungroup() |> 
  
  # pivot wider to get columns
  pivot_wider(
    id_cols = c(hospitalizations_joined_id, recorded_dttm, obs),
    names_from = lab_category,
    values_from = lab_value_numeric,
    # names_prefix = "lab_wide_",
    # values_fill = NA
  ) |>
  
  # Quick min/max for duplicates at the SAME time
  dplyr::select(-obs) |> 
  mutate(lab_date = as.Date(recorded_dttm, tz=user_timezone)) |> 
  distinct() 

#ni_tic()
# find the duplicates by day
df_laps2_prep2 <- df_laps2_prep1 |>
  # arrange(hospitalizations_joined_id, recorded_dttm) |>
  group_by(hospitalizations_joined_id, lab_date) |>
  # mutate(n = n()) |>
  # filter(n > 1) |>
  summarise(
    albumin             = fmin(albumin, na.rm = TRUE),
    anion_gap           = fmax(anion_gap, na.rm = TRUE),
    bilirubin_total     = fmax(bilirubin_total, na.rm = TRUE),
    bun                 = fmax(bun, na.rm = TRUE),
    bicarbonate         = fmin(bicarbonate, na.rm = TRUE),
    creatinine          = fmax(creatinine, na.rm = TRUE),
    glucose_serum       = fmin(glucose_serum, na.rm = TRUE),
    hematocrit          = fmax(hematocrit, na.rm = TRUE),
    lactic_acid         = fmax(lactic_acid, na.rm = TRUE),
    pco2_arterial       = fmax(pco2_arterial, na.rm = TRUE),
    po2_arterial        = fmax(po2_arterial, na.rm = TRUE),
    ph_arterial         = fmin(ph_arterial, na.rm = TRUE),
    platelet_count      = fmin(platelet_count, na.rm = TRUE),
    so2_arterial        = fmin(so2_arterial, na.rm = TRUE),
    sodium              = fmin(sodium, na.rm = TRUE),
    troponin            = fmax(troponin_i, na.rm = TRUE),
    wbc                 = fmin(wbc, na.rm = TRUE
    ), 

    .groups = "drop"
  ) |> 
  
    # bun_cr_ratio
  mutate(
    bun_cr_ratio = bun / creatinine
  ) |> 
  
    mutate(across(where(is.numeric) & !c(hospitalizations_joined_id), ~ na_if(.x, is.infinite(.x)))) |> 
  # get rid of duplicate rows
  distinct()
  # |> 
#   group_by(hospitalizations_joined_id) |> 
#   # carryforward after prelaps imputation
#     fill(c(
#       albumin,
#       anion_gap,
#       bilirubin_total,
#       bun,
#       bicarbonate,
#       creatinine,
#       glucose,
#       hematocrit,
#       lactic_acid,
#       pco2_arterial,
#       po2_arterial,
#       ph_arterial,
#       platelet_count,
#       so2_arterial,
#       sodium,
#       troponin,
#       wbc,
#       ), 
#       .direction = "down") |>
  
#  ni_toc()


## Labs day one
df_laps2_dayone_prep1 <- 
  df_encounter_laps2_temp1 |> 
  dplyr::select(hospitalizations_joined_id, admission_dttm, discharge_dttm, dt_24hours_after_admit) |> 
  distinct() |> 
  left_join(clif_labs |> 
              filter(lab_category %in% c("albumin",
                                     "anion_gap",
                                     "bilirubin_total",
                                     "bun",
                                     "bicarbonate",
                                     "creatinine",
                                     "glucose_serum",
                                     # "hematocrit",
                                     "hemoglobin",
                                     #"lactate",
                                     "lactic_acid",
                                     "pco2_arterial",
                                     "po2_arterial",
                                     "ph_arterial",
                                     "platelet_count",
                                     "so2_arterial",
                                     "sodium",
                                     "troponin_i",
                                     "wbc")) |>            
              dplyr::select(-lab_order_dttm),
            
            # join only if between the first 24 hours
            by = join_by(hospitalizations_joined_id, 
                         dt_24hours_after_admit >= recorded_dttm)
  ) |> 

  # making all hemoglobin into hematocrits
  mutate(lab_value_numeric = fifelse(lab_category == "hemoglobin", lab_value_numeric*3, lab_value_numeric)) |> 
  mutate(lab_category = fifelse(lab_category == "hemoglobin", "hematocrit", lab_category)) |> 
  mutate(lab_category = fifelse(lab_category == "lactate", "lactic_acid", lab_category)) |> 
  
  group_by(hospitalizations_joined_id, recorded_dttm, lab_category) |> 
  # using obs lets you get away with duplicates when going wider and can fix later!
  mutate(obs = row_number()) |> 
  ungroup() |> 
  
  # pivot wider to get columns
  pivot_wider(
    id_cols = c(hospitalizations_joined_id, recorded_dttm, obs, admission_dttm),
    names_from = lab_category,
    values_from = lab_value_numeric,
    # names_prefix = "lab_wide_",
    # values_fill = NA
  ) |>
  
  # Quick min/max for duplicates at the SAME time
  dplyr::select(-obs) |> 
  distinct() 

# get min/max
df_laps2_dayone_prep2 <- df_laps2_dayone_prep1 |>
  arrange(hospitalizations_joined_id, recorded_dttm) |> 
  
  # filter out those without a recorded_dttm so we don't add a date later
  filter(!is.na(recorded_dttm)) |> 
  # if we make "lab_date" the encounter date... then we are switching all labs within 24hr of admission to the "day 1" in a sneaky fashion
  mutate(lab_date = as.Date(admission_dttm, tz=user_timezone)) |> # make it all the same day 1
  group_by(hospitalizations_joined_id, lab_date) |>
  # mutate(n = n()) |>
  # filter(n > 1) |>
  summarise(
    albumin             = fmin(albumin, na.rm = TRUE),
    anion_gap           = fmax(anion_gap, na.rm = TRUE),
    bilirubin_total     = fmax(bilirubin_total, na.rm = TRUE),
    bun                 = fmax(bun, na.rm = TRUE),
    bicarbonate         = fmin(bicarbonate, na.rm = TRUE),
    creatinine          = fmax(creatinine, na.rm = TRUE),
    glucose_serum       = fmin(glucose_serum, na.rm = TRUE),
    hematocrit          = fmax(hematocrit, na.rm = TRUE),
    lactic_acid         = fmax(lactic_acid, na.rm = TRUE),
    pco2_arterial       = fmax(pco2_arterial, na.rm = TRUE),
    po2_arterial        = fmax(po2_arterial, na.rm = TRUE),
    ph_arterial         = fmin(ph_arterial, na.rm = TRUE),
    platelet_count      = fmin(platelet_count, na.rm = TRUE),
    so2_arterial        = fmin(so2_arterial, na.rm = TRUE),
    sodium              = fmin(sodium, na.rm = TRUE),
    troponin            = fmax(troponin_i, na.rm = TRUE),
    wbc                 = fmin(wbc, na.rm = TRUE
    ),

    .groups = "drop"
  ) |> 
  
    # bun_cr_ratio
  mutate(
    bun_cr_ratio = bun / creatinine
  ) |> 
  
    # replace inf with NAs
  mutate(across(where(is.numeric) & !c(hospitalizations_joined_id), ~ na_if(.x, is.infinite(.x)))) |> 

    # get rid of duplicate rows
  distinct() 



# switch out the day 1 stuff 
df_laps2_final <- df_laps2_prep2 |> anti_join(df_laps2_dayone_prep2 |> 
                              dplyr::select(hospitalizations_joined_id, lab_date)) |> 
  bind_rows(df_laps2_dayone_prep2) |> 
  arrange(hospitalizations_joined_id, lab_date) |> 
  

  # filter out NA for lab date
  filter(!is.na(lab_date))


## GCS
df_gcs_merge_ready <- clif_gcs |>
  mutate(vital_value = case_when(
    gcs_total %in% c(14, 15)                ~ 1,
    gcs_total %in% c(8, 9, 10, 11, 12, 13)  ~ 3,
    gcs_total %in% c(2, 3, 4, 5, 6, 7)      ~ 4,
    TRUE                                    ~ NA   # we account for this when doing the points later
  )) |>
  mutate(vital_category = "gcs") |> 
  dplyr::select(hospitalizations_joined_id,
         vital_category,
         recorded_dttm,
         vital_value,
         )


df_vitals_gcs <- clif_vitals |>
              filter(vital_category %in% c("temp_c",
                                       "sbp",
                                       "spo2",
                                       "heart_rate",
                                       "respiratory_rate")) |>
              # dplyr::select(-meas_site_name) |> 
              
                          # get gcs in there now
              bind_rows(df_gcs_merge_ready)  


mem.maxNSize(500000000) # CE ADDED - To prevent memory error

## vitals all
df_laps2_vitals_prep1 <- 
  df_encounter_laps2_temp1 |> 
  dplyr::select(hospitalizations_joined_id, admission_dttm, discharge_dttm) |>
  distinct() |> 
  left_join(df_vitals_gcs,
            
            # join by the encounter ID
            by = join_by(hospitalizations_joined_id)) |> 
  
  group_by(hospitalizations_joined_id, recorded_dttm, vital_category) |> 
  # using obs lets you get away with duplicates when going wider and can fix later!
  mutate(obs = row_number()) |>
  ungroup() |> 
  pivot_wider(
    id_cols = c(hospitalizations_joined_id, recorded_dttm, obs),
    names_from = vital_category,
    values_from = vital_value,
    # names_prefix = "vital_wide_",
    # values_fill = NA
  ) |>
  
  #fixing temp 
  mutate(temp = (temp_c * (9/5)) + 32) |> 
  
  #renaming resp
  rename(resp = respiratory_rate) |> 
  
  # Quick min/max for duplicates at the SAME time
  dplyr::select(-obs) |>
  distinct() 


# find the duplicates by day
df_laps2_vitals_prep2 <- df_laps2_vitals_prep1 |>
  arrange(hospitalizations_joined_id, recorded_dttm) |>
  mutate(vital_date = as.Date(recorded_dttm, tz=user_timezone)) |> 
  group_by(hospitalizations_joined_id, vital_date) |>
  # mutate(n = n()) |>
  # filter(n > 1) |>
  summarise(
    temp                = fmin(temp, na.rm = TRUE),
    sbp                 = fmin(sbp, na.rm = TRUE),
    spo2                = fmin(spo2, na.rm = TRUE),
    heart_rate               = fmax(heart_rate, na.rm = TRUE),
    resp                = fmax(resp, na.rm = TRUE),
    gcs                 = fmax(gcs, na.rm = TRUE),  # remember the groupings... higher is lower gcs
    # shock_index         = fmax(shock_index, na.rm = TRUE),  # do this after

    .groups = "drop"
  ) |> 
  mutate(
    shock_index = heart_rate / sbp
  ) |> 
  # replace inf with NAs
  mutate(across(where(is.numeric) & !c(hospitalizations_joined_id), ~ na_if(.x, is.infinite(.x)))) |> 

  # get rid of duplicate rows
  distinct() 


## vitals day one



df_laps2_vitals_dayone_prep1 <- 
  df_encounter_laps2_temp1 |> 
  dplyr::select(hospitalizations_joined_id, admission_dttm, discharge_dttm, dt_24hours_after_admit) |>
  distinct() |> 
  left_join(df_vitals_gcs,
            
            # join only if between the first 24 hours
            by = join_by(hospitalizations_joined_id, 
                         dt_24hours_after_admit >= recorded_dttm)
  ) |> 
  
    group_by(hospitalizations_joined_id, recorded_dttm, vital_category) |> 
  # using obs lets you get away with duplicates when going wider and can fix later!
  mutate(obs = row_number()) |> 
  ungroup() |> 
  pivot_wider(
    id_cols = c(hospitalizations_joined_id, recorded_dttm, obs, admission_dttm),
    names_from = vital_category,
    values_from = vital_value,
    # names_prefix = "vital_wide_",
    # values_fill = NA
  ) |>
  
  #fixing temp 
  mutate(temp = (temp_c * (9/5)) + 32) |> 
  
  #renaming resp
  rename(resp = respiratory_rate) |> 
  
  
  # Quick min/max for duplicates at the SAME time
  dplyr::select(-obs) |> 
  

  distinct() 

# find the duplicates by day
df_laps2_vitals_dayone_prep2 <- df_laps2_vitals_dayone_prep1 |>
  arrange(hospitalizations_joined_id, recorded_dttm) |>
  
  # filter out those without a recorded_dttm so we don't add a date later
  filter(!is.na(recorded_dttm)) |> 
  
  # if we make "lab_date" the encounter date... then we are switching all labs within 24hr of admission to the "day 1" in a sneaky fashion
  #using encounter admit date to collect everything together
  mutate(vital_date = as.Date(admission_dttm, tz=user_timezone)) |> 
  group_by(hospitalizations_joined_id, vital_date) |>
  # mutate(n = n()) |>
  # filter(n > 1) |>
  summarise(
    temp                = fmin(temp, na.rm = TRUE),
    sbp                 = fmin(sbp, na.rm = TRUE),
    spo2                = fmin(spo2, na.rm = TRUE),
    heart_rate          = fmax(heart_rate, na.rm = TRUE),
    resp                = fmax(resp, na.rm = TRUE),
    gcs                 = fmax(gcs, na.rm = TRUE),
    # shock_index         = fmax(shock_index, na.rm = TRUE),  # do this after

    .groups = "drop"
  ) |> 
  mutate(
    shock_index         = heart_rate / sbp
  ) |> 
  
  # replace inf with NAs
  mutate(across(where(is.numeric) & !c(hospitalizations_joined_id), ~ na_if(.x, is.infinite(.x)))) |> 

  # get rid of duplicate rows
  distinct() 

# switch out the day 1 stuff 
df_laps2_vitals_final <- df_laps2_vitals_prep2 |> anti_join(df_laps2_vitals_dayone_prep2 |> 
                              dplyr::select(hospitalizations_joined_id, vital_date)) |> 
  bind_rows(df_laps2_vitals_dayone_prep2) |> 
  arrange(hospitalizations_joined_id, vital_date) |> 
  
  # drop rows with no recorded date
  filter(!is.na(vital_date))



## Laps2 code
df_laps_calc <- df_laps2_final |> 
  # get same naming for date
  rename(recorded_date = lab_date) |> 
  full_join(df_laps2_vitals_final |> 
              rename(recorded_date = vital_date),
            join_by(hospitalizations_joined_id, recorded_date)
            ) |> 
  full_join(df_pre_laps_final)
  
clif_laps2_scores <- df_laps_calc |> 
  group_by(hospitalizations_joined_id, recorded_date) |> 
  
  mutate(laps2 = 0) |> 
  
  mutate(
    # lactic_acid and pH 
    laps2 = fcase(
      
      # missing data
      is.na(ph_arterial)  &                      is.na(lactic_acid) & high_risk == 0 , laps2 + 0,
      is.na(ph_arterial)  &                      is.na(lactic_acid) & high_risk == 1 , laps2 + 15,
      is.na(ph_arterial)  &                      lactic_acid < 2    & high_risk == 0 , laps2 + 0,
      is.na(ph_arterial)  &                      lactic_acid < 2    & high_risk == 1 , laps2 + 12,
      is.na(ph_arterial)  & lactic_acid >= 2   & lactic_acid < 4    & high_risk == 0 , laps2 + 12,
      is.na(ph_arterial)  & lactic_acid >= 2   & lactic_acid < 4    & high_risk == 1 , laps2 + 15,
      is.na(ph_arterial)  &                      lactic_acid >= 4   & high_risk == 0 , laps2 + 26,
      is.na(ph_arterial)  &                      lactic_acid >= 4   & high_risk == 1 , laps2 + 30,
      ph_arterial < 7.2   &                      is.na(lactic_acid) & high_risk == 0 , laps2 + 13,
      ph_arterial < 7.2   &                      is.na(lactic_acid) & high_risk == 1 , laps2 + 19,
      ph_arterial >= 7.2  & ph_arterial < 7.35 & is.na(lactic_acid) & high_risk == 0 , laps2 + 5,
      ph_arterial >= 7.2  & ph_arterial < 7.35 & is.na(lactic_acid) & high_risk == 1 , laps2 + 15,
      ph_arterial >= 7.35 & ph_arterial < 7.45 & is.na(lactic_acid) & high_risk == 0 , laps2 + 0,
      ph_arterial >= 7.35 & ph_arterial < 7.45 & is.na(lactic_acid) & high_risk == 1 , laps2 + 12,
      ph_arterial >= 7.45 &                      is.na(lactic_acid) & high_risk == 0 , laps2 + 12,
      ph_arterial >= 7.45 &                      is.na(lactic_acid) & high_risk == 1 , laps2 + 15,

      # complete data
      ph_arterial < 7.2   &                      lactic_acid <  2                   , laps2 + 13,
      ph_arterial < 7.2   & lactic_acid >= 2   & lactic_acid <  4                   , laps2 + 19,
      ph_arterial < 7.2   &                      lactic_acid >= 4                   , laps2 + 34,
      ph_arterial >= 7.2  & ph_arterial < 7.35 & lactic_acid <  2                   , laps2 + 5,
      ph_arterial >= 7.2  & ph_arterial < 7.35 & lactic_acid >= 2 & lactic_acid < 4 , laps2 + 15,
      ph_arterial >= 7.2  & ph_arterial < 7.35 & lactic_acid >= 4                   , laps2 + 25,
      ph_arterial >= 7.35 & ph_arterial < 7.45 & lactic_acid <  2                   , laps2 + 0,
      ph_arterial >= 7.35 & ph_arterial < 7.45 & lactic_acid >= 2 & lactic_acid < 4 , laps2 + 12,
      ph_arterial >= 7.35 & ph_arterial < 7.45 & lactic_acid >= 4                   , laps2 + 26,
      ph_arterial >= 7.45 &                      lactic_acid <  2                   , laps2 + 12,
      ph_arterial >= 7.45 & lactic_acid >= 2 &   lactic_acid <  4                   , laps2 + 15,
      ph_arterial >= 7.45 &                      lactic_acid >= 4                   , laps2 + 30,
      default                                                                       = laps2
      ),
    
    # Sodium
    laps2 = fcase(
      is.na(sodium)                                                             , laps2 + 0,
      sodium <  129                                                             , laps2 + 14,
      sodium >= 129 & sodium < 135                                              , laps2 + 7,
      sodium >= 135 & sodium < 146                                              , laps2 + 0,
      sodium >= 146                                                             , laps2 + 4,
      default                                                                   = laps2
    ),
    
    # Bilirubin
    laps2 = fcase(
      is.na(bilirubin_total)                                                    , laps2 + 0,
      bilirubin_total <  2                                                      , laps2 + 0,
      bilirubin_total >= 2 & bilirubin_total < 3                                , laps2 + 11,
      bilirubin_total >= 3 & bilirubin_total < 5                                , laps2 + 18,
      bilirubin_total >= 5 & bilirubin_total < 8                                , laps2 + 25,
      bilirubin_total >= 8                                                      , laps2 + 41,
      TRUE , laps2
    ),
    
    # BUN
    laps2 = fcase(
      is.na(bun)                                                                , laps2 + 0,
      bun < 18                                                                  , laps2 + 0,
      bun >= 18 & bun < 20                                                      , laps2 + 11,
      bun >= 20 & bun < 40                                                      , laps2 + 12,
      bun >= 40 & bun < 80                                                      , laps2 + 20,
      bun >= 80                                                                 , laps2 + 25,
      default                                                                   = laps2
    ),
    
    # Creatinine
    laps2 = fcase(
      is.na(creatinine)                                                         , laps2 + 0,
      creatinine < 1                                                            , laps2 + 0,
      creatinine >= 1 & creatinine < 2                                          , laps2 + 6,
      creatinine >= 2 & creatinine < 4                                          , laps2 + 11,
      creatinine >= 4                                                           , laps2 + 5,
      default                                                                   = laps2
    ),
    
    # BUN/Cr Ratio
    laps2 = fcase(
      is.na(bun_cr_ratio)                                                       , laps2 + 0,
      bun_cr_ratio < 25                                                         , laps2 + 0,
      bun_cr_ratio >= 25                                                        , laps2 + 10,
      default                                                                   = laps2
    ),
    
    # Albumin
    laps2 = fcase(
      is.na(albumin)                                                            , laps2 + 0,
      albumin < 2                                                               , laps2 + 31,
      albumin >= 2 & albumin < 2.5                                              , laps2 + 15,
      albumin >= 2.5                                                            , laps2 + 0,
      default                                                                   = laps2
    ),
    
    # glucose_serum
    laps2 = fcase(
      is.na(glucose_serum)                                                      , laps2 + 0,
      glucose_serum < 40                                                        , laps2 + 10,
      glucose_serum >= 40 & glucose_serum < 60                                  , laps2 + 10,
      glucose_serum >= 60 & glucose_serum < 200                                 , laps2 + 0,
      glucose_serum >= 200                                                      , laps2 + 3,
      default                                                                   = laps2
    ),
    
    # Hematocrit (Hct)
    laps2 = fcase(
      is.na(hematocrit)                                                         , laps2 + 0,
      hematocrit < 20                                                           , laps2 + 7,
      hematocrit >= 20 & hematocrit < 40                                        , laps2 + 8,
      hematocrit >= 40 & hematocrit < 50                                        , laps2 + 0,
      hematocrit >= 50                                                          , laps2 + 3,
      default                                                                   = laps2
    ),
    
    # WBC
    laps2 = fcase(
      is.na(wbc) &                                               high_risk == 0 , laps2 + 0,
      is.na(wbc) &                                               high_risk == 1 , laps2 + 32,
      wbc < 5                                                                   , laps2 + 8,
      wbc >= 5 & wbc < 13                                                       , laps2 + 0,
      wbc >= 13                                                                 , laps2 + 11,
      default                                                                   = laps2
    ),
    
  # pco2_arterial
    laps2 = fcase(
      is.na(pco2_arterial)                                                      , laps2 + 0,
      pco2_arterial < 35                                                        , laps2 + 7,
      pco2_arterial >= 35 & pco2_arterial < 45                                  , laps2 + 0,
      pco2_arterial >= 45 & pco2_arterial < 55                                  , laps2 + 11,
      pco2_arterial >= 55 & pco2_arterial < 65                                  , laps2 + 13,
      pco2_arterial >= 65                                                       , laps2 + 12,
      default                                                                   = laps2
    ),
  
  # po2_arterial
    laps2 = fcase(
      is.na(po2_arterial)                                                       , laps2 + 0,
      po2_arterial < 50                                                         , laps2 + 8,
      po2_arterial >= 50 & po2_arterial < 120                                   , laps2 + 0,
      po2_arterial >= 120                                                       , laps2 + 12,
      default                                                                   = laps2
    ),
  
  # Troponin
    laps2 = fcase(
      is.na(troponin)  &                                       high_risk == 0 , laps2 + 0,
      is.na(troponin)  &                                       high_risk == 1 , laps2 + 9,
      troponin <  0.01                                                        , laps2 + 0,
      troponin >= 0.01 & troponin < 0.2                                       , laps2 + 8,
      troponin >= 0.2  & troponin < 1                                         , laps2 + 17,
      troponin >= 1    & troponin < 3                                         , laps2 + 19,
      troponin >= 3                                                           , laps2 + 25,
      default                                                                   = laps2
    ),
  
  # Temp
    laps2 = fcase(
      is.na(temp)                                                               , laps2 + 0,
      temp < 96                                                                 , laps2 + 20,
      temp >= 96    & temp < 100.5                                              , laps2 + 0,
      temp >= 100.5                                                             , laps2 + 3,
      default                                                                   = laps2
    ),

  # heart_rate
    laps2 = fcase(
      is.na(heart_rate)                                                         , laps2 + 0,
      heart_rate < 60                                                           , laps2 + 7,
      heart_rate >= 60  & heart_rate < 110                                      , laps2 + 0,
      heart_rate >= 110 & heart_rate < 140                                      , laps2 + 7,
      heart_rate >= 140                                                         , laps2 + 10,
      default                                                                   = laps2
    ),
  
  # Resp
    laps2 = fcase(
      is.na(resp)                                                               , laps2 + 0,
      resp < 20                                                                 , laps2 + 0,
      resp >= 20 & resp < 30                                                    , laps2 + 11,
      resp >= 30                                                                , laps2 + 21,
      default                                                                   = laps2
    ),
    
  # SBP
    laps2 = fcase(
      is.na(sbp)                                                                , laps2 + 0,
      sbp < 75                                                                  , laps2 + 22,
      sbp >= 75  & sbp < 90                                                     , laps2 + 13,
      sbp >= 90  & sbp < 120                                                    , laps2 + 5,
      sbp >= 120 & sbp < 140                                                    , laps2 + 0,
      sbp >= 140 & sbp < 160                                                    , laps2 + 8,
      sbp >= 160                                                                , laps2 + 14,
      default                                                                   = laps2
    ),
  
  # Shock
    laps2 = fcase(
      is.na(shock_index)                                                        , laps2 + 0,
      shock_index < 0.65                                                        , laps2 + 0,
      shock_index >= 0.65 & shock_index < 0.85                                  , laps2 + 8,
      shock_index >= 0.85                                                       , laps2 + 17,
      default                                                                   = laps2
    ),
  
  # O2Sat
    laps2 = fcase(
      is.na(spo2) &                                              high_risk == 0 , laps2 + 0,
      is.na(spo2) &                                              high_risk == 1 , laps2 + 22,
      spo2 <  90                                                                , laps2 + 22,
      spo2 >= 90  & spo2 < 94                                                   , laps2 + 12,
      spo2 >  94                                                                , laps2 + 0,
      default                                                                   = laps2
    ),
  
  # Neuro (GCS)
    laps2 = fcase(
      is.na(gcs) &                                               high_risk == 0 , laps2 + 16,
      is.na(gcs) &                                               high_risk == 1 , laps2 + 21,
      gcs == 1                                                                  , laps2 + 0,
      gcs == 2                                                                  , laps2 + 16,
      gcs == 3                                                                  , laps2 + 21,
      gcs == 4                                                                  , laps2 + 36,
      default                                                                   = laps2
    )
  )

  # CE added below to create a laps2 column joined to the input dataframe
  laps2 <- clif_laps2_scores |>
      dplyr::select(c(hospitalizations_joined_id, laps2)) |>
      distinct()

  data <- data |>
      left_join(laps2, by = join_by(hospitalizations_joined_id, recorded_date))
  
  return(data)
}
