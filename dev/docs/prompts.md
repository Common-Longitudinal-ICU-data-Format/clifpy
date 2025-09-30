# docs

review all the files under @examples/ and @docs/user-guide/  and synthesize them into a marino notebook at examples/demo.py which is serves as a quick tutorial of the core functionalities of the package for first time users. some of the files you are asked to review may contain zombie code that no longer works, so please also keep them in mind as future targets for update/removal and please do not include them in demo.py. for example, the most recommended way of usage is through the ClifOrchestrator class so make sure you center the tutorial demo.py around that. Note that that a lot of code in the demo notebook is expected to pass automatic `doctest` if they are transported into code blocks in a markdown file. make sure you use context7 to understand the coding patterns and requirement of a marimo notebook. 

provide a plan of implementation including a list of the core functionalities you identified

1. can you devise a plan to update the interface of using the demo data, such that it would maximally resemble how the functions/classes should be invoked when users actually use their own data. I'm thinking something like `co = ClifOrchestrator(config_path='config/demo_data_config.yaml')` instead of the current `co = load_demo_clif(timezone = 'US/Central')`.
 
2. update your current implementation plan for the demo.py marimo notebook with the above understanding and that a lot of code in the demo notebook is expected to pass automatic `doctest` if they are transported into code blocks in a markdown file.

3. make sure you use context7 to understand the coding patterns and requirement of a marimo notebook. 

1. remove any mentioning of json, we are going full .yaml.
2. use `co` to consistently refer to the instantiated ClifOrchetrator. 
3. for doctest, do not 'skip blocks requiring large data.'
4. refer to @clifpy/utils/config.py for all the ways of configuration. make sure you cover all the possible approaches (e.g. using a .yaml file or using the args of the builder)
5. i'm thinking of removing the orchestrator.md file entirely and move the content elsewhere. Overall i'm thinking there should be one must-read page for every user that introduces the core features, like a clifpy 101 course (not that it should be necessarily named that way). this must-read intro page should introduce the orchestrator conceptually and its core features (including e.g. wide dataset creation, sofa calculation, med dose unit conversion, etc.) and point to their respective pages for more specifics. should these be in basic-usage.md? what are your recommendations? I also agree that the overview index.md page should be more like a table of contents with just a brief intro of the package and then point to other pages with brief explanations.

overall good, but 
1. i want to move quickstart.md to be within basic-usage.md as well -- can it just be a subsection in it? perhaps the first? 
2. all the demo code should thrive to demo the function interfaces in two steps: first, the minimal args needed; second, the most common and useful args all configured at once. e.g. the orchestrator can load the data via `co.initialize(tables=['patient', 'vitals', 'labs'])` but more commonly we'd want users to use the `columns` and `filters` args as well to reduce the memory burden created by excessive data loaded. For all such specifications, refer to @clifpy/clif_orchestrator.py
3. meanwhile, where do you plan to mention the configuration options?

# `utils`
## `unit_converter`
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

1. can you make the file called med-unit-conversion.md to avoid confusion\
2. can you create a table for all the acceptable units? the columns would be: unit class; unit subclass;
_clean_unit; acceptable variations;_base_unit. and explain where you see fit what they mean, i.e. unit
class = rate or amount; unit subclass = mass, volume, unit; _clean_unit = the format in which user need to
write their preferred units; acceptable variations = all of 'mL/m' 'ml/min' 'milli-liter/minute' would be
converted to the 'ml/min'. finally mention that the unit class and subclass are used to track if units
are conversion to one each other. 
3. introduce how the _convert_status column should be used and how any failure in conversion would result
in the original dose and _clean_unit being the *_converted output columns and the override = True option
can be used to bypass detection of unacceptable conversion\
4. highlight both the public functions give two outputs: a df with converted dose and units and a df that
lists the conversion status and count by med_category, med_unit, _base_unit, etc etc.