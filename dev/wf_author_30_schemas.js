export const meta = {
  name: 'author-clif-30-schemas',
  description: 'Author CLIF 3.0 schema YAMLs for all 41 tables from DDL + mCIDE, then adversarially verify each',
  phases: [
    { title: 'Author', detail: 'one agent per table: DDL + mCIDE CSV + 2.1 template -> 3.0 YAML' },
    { title: 'Verify', detail: 'adversarial check of each authored YAML against sources' },
  ],
}

// args is the list of table names (41). Each agent discovers its own files by convention.
const TABLES = Array.isArray(args) ? args : JSON.parse(args)

const RULES = `
You are authoring ONE CLIF 3.0 table schema YAML for the clifpy package. Be exact and faithful to the sources; do NOT invent clinical data.

## Source files (read them with your tools)
- DDL slice for this table: /tmp/ddl/<TABLE>.sql  (authoritative for columns, order, SQL types, and the JSON COMMENT per column with "description" and "permissible").
- mCIDE controlled-vocabulary CSVs: list them with: ls /tmp/mCIDE/<TABLE>/*.csv  (NOTE: the 'position' table's dir is misspelled '/tmp/mCIDE/postion/'). Some tables have NO mCIDE dir — then permissible values come ONLY from the inline list in the DDL COMMENT (e.g. "[a, b, c](url)") or are free-text/"No restriction".
- 2.1 template (existing tables only): clifpy/schemas/2.1/<TABLE>_schema.yaml  (for the renamed table 'mcs', the 2.1 template is clifpy/schemas/2.1/ecmo_mcs_schema.yaml — but 'mcs' is REDESIGNED in 3.0, so use the DDL/mCIDE as primary truth and the template only for FORMAT). If no 2.1 template exists, this is a NEW 3.0 table — use clifpy/schemas/2.1/position_schema.yaml purely as a FORMAT reference.

## Output
Write the file: clifpy/schemas/3.0/<TABLE>_schema.yaml  (use the Write tool). It MUST parse as valid YAML.

## Exact format (match the 2.1 schemas)
Top-level keys in this order:
  table_name: <table>
  version: "3.0"
  composite_keys:            # include ONLY if the 2.1 template had it, or the DDL indicates a composite key (e.g. descriptions saying "Together with hospitalization_id, forms the composite key"). Otherwise omit.
    - <col>
  columns:                   # one entry per DDL column, IN DDL ORDER
    - name: <col>
      data_type: <VARCHAR|DATETIME|DATE|INT|DOUBLE>
      required: <true|false>
      is_category_column: <true|false>
      is_group_column: <true|false>
      permissible_values:    # ONLY for columns that have a controlled vocabulary
        - <value>
  required_columns:          # list of column names where required: true
    - <col>
  category_columns:          # list of column names where is_category_column: true
    - <col>
  group_columns:             # list of column names where is_group_column: true (use [] if none)
  <extra metadata sections>  # see below

## data_type mapping (DDL SQL type -> schema type)
- VARCHAR -> VARCHAR ; DATETIME -> DATETIME ; DATE -> DATE ; INT -> INT
- FLOAT -> DOUBLE  (clifpy convention; the validator treats them identically)
- BOOLEAN -> INT   (stored as 0/1 flags)

## required / category / group inference (CRITICAL)
- IMPORTANT clifpy rule: columns ending in '_name' are raw SOURCE TEXT and are OPTIONAL -> required: false. Columns ending in '_category' are standardized and REQUIRED -> required: true. (Do not let any external dictionary override this.)
- A column is REQUIRED (required: true) by default, EXCEPT: any '_name' column; any column whose DDL description contains "(Optional)" / "optional" / "nullable" / "if applicable"; and clearly-optional numeric/text value fields the DDL marks optional. ID keys, primary dttm columns, '*_category' columns, and the primary value column are required.
- is_category_column: true for '*_category' columns that have a controlled vocabulary (permissible_values). false otherwise.
- is_group_column: true for '*_group' columns; set is_category_column: false for those (a column is either category or group, not both). Give '*_group' columns their permissible_values too.
- For EXISTING tables: PREFER the 2.1 template's required/category/group flags for columns that carry over unchanged; only deviate when 3.0 clearly changed the column. This preserves established behavior.

## permissible_values
- If the DDL COMMENT permissible field is an inline list like "[a, b, c](url)", use exactly those values.
- If it says "[List of ... in CLIF](url)" (no inline values), READ the corresponding mCIDE CSV and take the values from its FIRST column (the *_category / *_group / *_name code column — usually column 1; inspect the header). Use the snake_case code values, not descriptions.
- ALL 3.0 permissible values are lowercase snake_case. If a CSV value isn't, snake_case it, but prefer the CSV's literal code values (they are already snake_case in 3.0).
- For free-text / "No restriction" / "Under-development" / "WIP" category columns with no enumerable vocabulary, set is_category_column: false and DO NOT add permissible_values (treat as a plain VARCHAR). Note this in uncertainties.

## extra metadata sections (PRESERVE downstream-critical data)
Some 2.1 schemas carry extra top-level sections that clifpy code reads. For EXISTING tables you MUST carry these over, UPDATING entries for changed/added categories using the mCIDE CSV. Specifically:
- vitals: 'vital_units' and 'vital_ranges' — carry from template; add entries for new categories (intracranial_pressure, pulse_pressure_variation) ONLY if you can source a real unit/range (the vitals CSV descriptions state units, e.g. ICP mmHg, PPV %). If you cannot source a numeric range, OMIT that range entry and list it in uncertainties — do NOT fabricate ranges.
- labs: 'lab_reference_units' (REBUILD from /tmp/mCIDE/labs/clif_lab_categories.csv 'reference_unit' column, keyed by lab_category), and carry over 'lab_outlier_ranges', 'lab_reference_ranges', 'allowed_unit_variants' from the 2.1 template for labs that still exist. New labs without template ranges: omit and note.
- medication_admin_continuous & medication_admin_intermittent: 'med_category_to_group_mapping' — REBUILD from the med_categories CSV (it maps each med_category to its med_group). Preserve the template's structure (a value may map to a list of groups).
- If the template has any other extra section, carry it over, updating for renamed/added categories.

## Return value (StructuredOutput)
Report what you did: table, wrote (true/false), n_columns, the category_columns and group_columns lists, the names of any extra metadata sections you wrote, and an 'uncertainties' list of anything you could not source authoritatively (missing ranges, ambiguous required-status, free-text categories, etc.). Keep uncertainties specific.
`

const AUTHOR_SCHEMA = {
  type: 'object',
  properties: {
    table: { type: 'string' },
    wrote: { type: 'boolean' },
    n_columns: { type: 'integer' },
    category_columns: { type: 'array', items: { type: 'string' } },
    group_columns: { type: 'array', items: { type: 'string' } },
    extra_sections: { type: 'array', items: { type: 'string' } },
    uncertainties: { type: 'array', items: { type: 'string' } },
  },
  required: ['table', 'wrote', 'n_columns', 'category_columns', 'group_columns', 'uncertainties'],
  additionalProperties: false,
}

const VERIFY_SCHEMA = {
  type: 'object',
  properties: {
    table: { type: 'string' },
    severity: { type: 'string', enum: ['pass', 'warn', 'fail'] },
    parses: { type: 'boolean' },
    issues: { type: 'array', items: { type: 'string' } },
  },
  required: ['table', 'severity', 'parses', 'issues'],
  additionalProperties: false,
}

const results = await pipeline(
  TABLES,
  // Stage 1: author
  (table) => agent(
    `${RULES}\n\n## YOUR TABLE: ${table}\nAuthor clifpy/schemas/3.0/${table}_schema.yaml now. First inspect /tmp/ddl/${table}.sql, then \`ls /tmp/mCIDE/${table}/*.csv\` (use /tmp/mCIDE/postion for 'position'), read any CSVs and the 2.1 template, then Write the file.`,
    { label: `author:${table}`, phase: 'Author', schema: AUTHOR_SCHEMA }
  ),
  // Stage 2: adversarial verify (runs as soon as each table is authored)
  (authored, table) => agent(
    `Adversarially verify the CLIF 3.0 schema file clifpy/schemas/3.0/${table}_schema.yaml that was just written.
Check against the sources:
- DDL: /tmp/ddl/${table}.sql  (every DDL column must be present, in order, correct data_type per the mapping VARCHAR/DATETIME/DATE/INT/DOUBLE with FLOAT->DOUBLE and BOOLEAN->INT)
- mCIDE CSVs: ls /tmp/mCIDE/${table}/*.csv (use /tmp/mCIDE/postion for 'position'). For each '*_category'/'*_group' column with a controlled vocab, the permissible_values must match the CSV's code column (or the DDL inline list), all lowercase snake_case.
- 2.1 template (if clifpy/schemas/2.1/${table}_schema.yaml or ecmo_mcs for mcs exists): downstream-critical extra sections (vital_units/vital_ranges; lab_reference_units/allowed_unit_variants/lab_outlier_ranges/lab_reference_ranges; med_category_to_group_mapping) must be present and updated, NOT dropped.
Verify: (1) file parses as YAML; (2) all DDL columns present with correct types; (3) required/category/group flags follow the rules (_name optional, _category required, _group -> is_group_column); (4) permissible_values correct & snake_case; (5) required_columns/category_columns/group_columns lists are consistent with the per-column flags; (6) extra metadata sections preserved for existing tables; (7) NO fabricated clinical ranges/units.
Read the file and the sources yourself. Report severity: 'fail' for missing/wrong columns, bad types, dropped extra sections, fabricated data, or YAML parse errors; 'warn' for minor/justifiable deviations; 'pass' if correct. List concrete issues with column names.`,
    { label: `verify:${table}`, phase: 'Verify', schema: VERIFY_SCHEMA }
  ).then(v => ({ ...v, author: authored }))
)

const clean = results.filter(Boolean)
const fails = clean.filter(r => r.severity === 'fail')
const warns = clean.filter(r => r.severity === 'warn')
log(`Authored ${clean.length}/${TABLES.length} tables. fail=${fails.length} warn=${warns.length} pass=${clean.length - fails.length - warns.length}`)

return {
  total: TABLES.length,
  authored: clean.length,
  fail: fails.map(r => ({ table: r.table, issues: r.issues })),
  warn: warns.map(r => ({ table: r.table, issues: r.issues })),
  uncertainties: clean
    .filter(r => r.author && r.author.uncertainties && r.author.uncertainties.length)
    .map(r => ({ table: r.table, uncertainties: r.author.uncertainties })),
}
