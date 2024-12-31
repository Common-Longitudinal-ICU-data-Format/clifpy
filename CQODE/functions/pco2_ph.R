# ===== pco2_ph() =====
# Message that describes function and explains what it does

# Input: *Function inputs
# Output: *What is output by the function

pco2_ph <- function(data=NULL, clif_labs=NULL) {
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

    data$hospitalizations_joined_id <- as.character(data$hospitalizations_joined_id)
    clif_labs$hospitalizations_joined_id <- as.character(clif_labs$hospitalizations_joined_id)

    #user_timezone <- Sys.timezone()
    #data$recorded_date <- as.Date(data$recorded_date_timestamp, tz = user_timezone)

    # Assuming CLIF labs table contains pH and pCO2 values along with recorded_dttm
    # Prepare the labs data
    clif_labs_htvv <- clif_labs %>%
    filter(lab_category %in% c("ph_arterial", "pco2_arterial")) %>%  # Filter for pH and pCO2 from the labs
    mutate(
        recorded_date = date(lab_result_dttm),  # Extract date from recorded timestamp
        recorded_hour = hour(lab_result_dttm)  # Extract hour from timestamp
    ) |>
    mutate(
        ph = fifelse(lab_category == "ph_arterial", lab_value_numeric, NA_real_),  # Assuming lab_name stores 'pH'
        pco2 = fifelse(lab_category == "pco2_arterial", lab_value_numeric, NA_real_)  # Assuming lab_name stores 'pCO2_arterial'
    ) |>
    dplyr::select(
        hospitalizations_joined_id,
        recorded_date,
        #recorded_hour,
        ph,
        pco2
    ) |>
    #  getting it so that there is one row per hour - !!! CE CHANGED TO PER DAY!!!
    #group_by(hospitalizations_joined_id,
    #         recorded_date, recorded_hour) |>
    group_by(hospitalizations_joined_id,
            recorded_date) |>
    mutate(
        ph = ffirst(ph, na.rm = TRUE),
        pco2 = ffirst(pco2, na.rm = TRUE) #replace first with max()?
    ) |>
    ungroup() |>
    distinct()

    # CE added to join ph and pco2 columns to original dataframe

    data <- data |>
        left_join(clif_labs_htvv, by = join_by(hospitalizations_joined_id, recorded_date))


    return(data)
}