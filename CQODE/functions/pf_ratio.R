# ===== pf_ratio() =====
pf_ratio <- function(data=NULL, clif_labs=NULL, clif_vitals=NULL) {
    # Check for dataframe existance
    if (is.null(data)) {
        if (exists("data", envir = .GlobalEnv)) {
        data <- get("data", envir = .GlobalEnv)
        print('No |data| input provided. Global |data| dataframe is being used')
        } else {
        stop("|data| not provided and no global |data| found.")
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
    if (is.null(clif_vitals)) {
        if (exists("clif_vitals", envir = .GlobalEnv)) {
        clif_vitals <- get("clif_vitals", envir = .GlobalEnv)
        print('No |clif_vitals| input provided. Global |clif_vitals| dataframe is being used')
        } else {
        stop("|clif_vitals| not provided and no global |clif_vitals| found.")
        }
    }

    # Make sure the recorded_date is correct!
   # data$recorded_date = date(data$recorded_date_timestamp)

    # Convert hospitalizations_joined_id to strings
    data$hospitalizations_joined_id <- as.character(data$hospitalizations_joined_id)
    clif_labs$hospitalizations_joined_id <- as.character(clif_labs$hospitalizations_joined_id)
    clif_vitals$hospitalizations_joined_id <- as.character(clif_vitals$hospitalizations_joined_id)
    print('String conversion done')

    # Right join to only keep data corresponding to the main regression data
    #clif_labs <- clif_labs |>
    #right_join(data |> dplyr::select(hospitalizations_joined_id) |> dplyr::distinct())
    #clif_vitals <- clif_vitals |>
    #right_join(data |> dplyr::select(hospitalizations_joined_id) |> dplyr::distinct())
    #df_hourly_resp_support <- df_hourly_resp_support |>
    #right_join(data |> dplyr::select(hospitalizations_joined_id) |> dplyr::distinct())


    # Start Nick's code!!
    clif_po2_arterial <- clif_labs |> filter(lab_category == "po2_arterial") |> rename(po2_arterial = lab_value_numeric)
    clif_spo2 <- clif_vitals |> filter(vital_category == "spo2") |> rename(spo2 = vital_value)
    clif_fio2 <- df_hourly_resp_support |> filter(!is.na(fio2_set)) |> dplyr::select(clif_hospitalizations_joined_id, recorded_date, recorded_hour, fio2_set, location_name, location_category)

    df_sf_ratio <- clif_fio2 |>
    # left_join because we want to keep everything right now
    left_join(clif_spo2 |>
                mutate(recorded_date = date(recorded_dttm),
                        recorded_hour = hour(recorded_dttm)) |>
                arrange(clif_hospitalizations_joined_id, recorded_date, recorded_hour, spo2) |>
                # taking first non-missing
                distinct(clif_hospitalizations_joined_id, recorded_date, recorded_hour, .keep_all = TRUE),
                by = join_by(clif_hospitalizations_joined_id, recorded_date, recorded_hour)
    ) |>
    dplyr::select(clif_hospitalizations_joined_id, recorded_date, recorded_hour, recorded_dttm, fio2_set, spo2, location_name, location_category) |>
    mutate(sf_ratio = spo2 / (fio2_set/100))

    df_pf_ratio <- df_sf_ratio |>
    left_join(clif_po2_arterial |>
                mutate(recorded_date = date(recorded_dttm),
                        recorded_hour = hour(recorded_dttm)) |>
                # keeping lab_name in there so ELS readings will be after normal pao2 readings
                #        ELS are sometimes falsely high
                arrange(clif_hospitalizations_joined_id, recorded_date, recorded_hour, lab_name, po2_arterial) |>
                # taking first non-missing
                distinct(clif_hospitalizations_joined_id, recorded_date, recorded_hour, .keep_all = TRUE),
                by = join_by(clif_hospitalizations_joined_id, recorded_date, recorded_hour)
    ) |>
    mutate(pf_ratio = po2_arterial / (fio2_set/100)) |>
    dplyr::select(clif_hospitalizations_joined_id,
            recorded_date,
            recorded_hour,
            fio2_set,
            spo2,
            po2_arterial,
            pf_ratio,
            sf_ratio,
            location_name,
            location_category)

      # CE added below to create a laps2 column joined to the input dataframe
    laps2 <- clif_laps2_scores |>
        dplyr::select(c(hospitalizations_joined_id, laps2)) |>
        distinct()

    data <- data |>
        left_join(laps2, by = join_by(hospitalizations_joined_id, recorded_date))
    return(data)
}

