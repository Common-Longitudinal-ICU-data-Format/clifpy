## `medication_admin_continuous`
Implement the following tests in tests/tables/test_medication_admin_continuous.py following instructions in their docstrings:
- test_acceptable_dose_unit_patterns
- test_standardize_dose_unit_pattern
- test_convert_dose_to_same_units

Use pytest features to make the tests more readable and less verbose.
If helpful, follow the pattern of existing tests in the same file.

the overall pattern looks good but please improve on the following issues:
1. the test data are not easily human readable and verificable. Can you create them as .csv's under 
tests/fixtures? the csv would look like the output df which contains the original df and the newly generated
appended columns. e.g. for test_convert_dose_to_same_units, the csv would have the following columns:
['med_dose', 'med_dose_unit_clean', 'weight_kg', 'med_dose_converted', 'med_dose_unit_converted']
2. within the same test function, we are testing multiple scenarios. Does it make sense to split them up into
separate test functions? I'm not sure about it either way. I think splitting them up seems like a good pattern
but I worry about code duplication and overcomplicating things. Can you advise and implement what you think would
be a preferred strategy, esp. taking into account the first issue above?

it looks good overall! but would it make sense to consolidate related test data into the same csv's and 
just add a new column like 'case' to differentiate between the different scenarios? i.e. I think i'd make sense
for me to be able to view the valid and invalid unit cases at the same time in the same csv, and we can just load
the csv as a whole and then filter by 'case'. Of course, for tests that are rather distinct and would require different columns, they should be still in different
csv's.

1. remove dose_unit_patterns.csv as that test does not necessarily need a tabular structure and the 
test data can be easily understood as two lists written in the .py file directly.
2. rename the csv to be aligned with the test names
3. modify column names in the csv's to be exactly aligned with the dataframes, e.g. 'expected_clean'
should be 'med_dose_unit_clean'; 'expected_dose_converted' should just be 'med_dose_converted'
4. remove unnecessary columns in the csv's such as 'med_category' which does not impact unit conversion
at all. 

Based on these two files: @tests/tables/test_medication_admin_continuous.py                                            │
@clifpy/tables/medication_admin_continuous.py
1. Add a test(s) for when no med_df was ever provided (self.df was not even populated), triggering the ValueError("No data provided")
2. Flesh out the doc strings for all functions