Product Requirements Document (PRD) – ICU Mortality Prediction

1. Objective
Develop a standardized pipeline using pyCLIF to:

Generate a wide hourly dataset for the first 24 hours of ICU stay.

Predict in-hospital mortality using two modeling approaches:

GBM model: Aggregated features over 24 hours into a single row per patient.

LSTM model: Full 24-hour hourly time series per patient.


2. Input Data and Cohort Definition
Cohort Table:

Contains:

hospitalization_id

start_dttm: ICU admission timestamp

hour_24_start_dttm: first ICU hour (may equal start_dttm)

hour_24_end_dttm: end of the first 24 hours

disposition: binary outcome (1 = expired, 0 = survived)
Cohort Criteria:

First 24 hours of first ICU stay

Exclude re-admissions and ICU readmissions



3. Feature Tables and Variables
Data Sources (CLIF tables):

vitals: All vital_category values

labs: All lab_category values

patient_assessments: GCS_total, RASS

respiratory_support: Mode, FiO2, PEEP, ventilator settings

medication_admin_continuous: All vasoactives and sedatives

<!-- dialysis: Binary indicators for support -->

Time Frame: Only data between hour_24_start_dttm and hour_24_end_dttm


4. Data Output Specification
GBM Model Dataset
One row per hospitalization_id

Each feature aggregated over 24 hours:

min, max, mean, std for continuous

last value, count for categorical

binary flags (e.g., received mechanical ventilation, ECMO, dialysis)

LSTM Model Dataset
24 rows per patient (one per hour)

Features:

Hourly values for vitals, labs, medications, support devices

Missing values imputed or forward-filled

Time-aligned and padded where necessary

5. Modeling
GBM Model
Input: 24-hour aggregated features

Algorithm: LightGBM or XGBoost

Evaluation: AUROC, AUPRC, Calibration

LSTM Model
Input: Hourly time series

Architecture: Multi-layer LSTM + Dense output

Evaluation: AUROC, AUPRC, sequence-level classification

6. Implementation Plan
Step 1: Cohort Extraction
Use ICU adt entries with location_category = 'ICU'

Find first ICU stay per hospitalization_id

Step 2: Data Extraction via pyCLIF
Build pyclif pipeline to extract features from CLIF tables

Filter using 24-hour time windows

Step 3: Feature Engineering
Hourly binning and summary stats for GBM

Time-series alignment for LSTM

Step 4: Dataset Output
Export GBM-ready CSV

Export LSTM-ready NumPy array or Tensor dataset

9. Model Saving and Reuse
Model Persistence
GBM Model:

Save trained model using joblib or native .model format

LSTM Model:

Save using torch.save() (for PyTorch) or model.save() (for Keras)

Include architecture + weights + preprocessing pipeline

Metadata file to include:

Feature schema

Model hyperparameters

Performance metrics on training and validation sets

10. Transfer Learning
Objective: Adapt pre-trained LSTM or GBM models to new site-specific data

Approach:
Use previously trained base model from source site

Fine-tune on local target-site dataset:

Freeze base layers (optional)

Re-train final layer or whole model with reduced learning rate

Evaluate performance before/after fine-tuning

11. Federated Learning
Objective: Train models collaboratively across multiple sites without sharing raw data

Methodology:
Use a federated learning library (e.g., Flower, PySyft, NVIDIA Clara)

Each site:

Loads local 24-hour hourly feature data

Trains local model on-site

Sends model weights (not data) to central server

Server:

Aggregates weights (e.g., FedAvg)

Distributes updated model back to sites

Repeat for defined number of rounds

Evaluation:
Compare performance:

Centralized training vs. fine-tuned vs. federated models

On both local and pooled test sets


