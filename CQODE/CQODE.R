#   _____  ____   ____    ___    ____        __    ____   ___    ___    ___    ___ __  __
#  / ___/ / __ \ / __ \  / _ \  / __/       / /   /  _/  / _ )  / _ \  / _ |  / _ \\ \/ /
# / /__  / /_/ // /_/ / / // / / _/        / /__ _/ /   / _  | / , _/ / __ | / , _/ \  / 
# \___/  \___\_\\____/ /____/ /___/       /____//___/  /____/ /_/|_| /_/ |_|/_/|_|  /_/  
#                                                                                        


# ===== 0. Notes/Reminders =====
# Headings and functions denoted by "# ===== *Heading/function* ====="
# Can use "args(*function)" to get the arguments for function and can be used in main code (ie. outside the library script)



# ===== 1. Dependencies to import =====
# Add packages here as needed - warning, if using base R environment - might install a new version
packages <- c("lme4","tidyverse","ggthemes","styler","readxl","writexl","DBI","dbplyr","knitr","pandoc","janitor", "data.table", "duckdb","powerjoin","collapse","tidyfast",
              "datapasta","fst","dtplyr","bit64","zoo","fuzzyjoin","arrow","hrbrthemes","here","table1", "rvest", "tidymodels", "pscl", "mice", "gt", "dplyr", "gtsummary")
install_if_missing <- function(package) {
  if (!require(package, character.only = TRUE)) {
    install.packages(package, dependencies = TRUE)
    library(package, character.only = TRUE)
  }
}
sapply(packages, install_if_missing)



# ===== 2. Functions to import =====
source('Q:/CQODE_SUPERUSERS/Workspace/Casey/Nick_Ingraham/R_Library/CQODE_/functions/demo.R')
source('Q:/CQODE_SUPERUSERS/Workspace/Casey/Nick_Ingraham/R_Library/CQODE_/functions/laps2.R')
source('Q:/CQODE_SUPERUSERS/Workspace/Casey/Nick_Ingraham/R_Library/CQODE_/functions/laps2_date.R')
source('Q:/CQODE_SUPERUSERS/Workspace/Casey/Nick_Ingraham/R_Library/CQODE_/functions/pf_ratio.R')
source('Q:/CQODE_SUPERUSERS/Workspace/Casey/Nick_Ingraham/R_Library/CQODE_/functions/pco2_ph.R')
#source(*function...)