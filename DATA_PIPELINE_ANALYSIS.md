# Amazon Data Processing – Pipeline Analysis

## 1. Folder Structure Mapping

Your **Magazine_Subscriptions** category structure maps to script usage as follows. Scripts use slightly different names (and one typo); both your names and the script names are listed.

| Your folder name           | Script / logic usage                    | Used by |
|----------------------------|-----------------------------------------|---------|
| **Mat_Files**              | Input: raw chunked .mat from JSONL      | Amazon_data_script (writes), reduced_form_script (reads) |
| **Processed_MAT_files**    | Processed “reduced form” .mat           | reduced_form_script (writes), reduced_form2matrix (reads). Scripts use **Procceced_Mat_Files** (typo) |
| **Matrix**                 | Numeric review matrix .mat               | reduced_form2matrix (writes), Matrix2csv (reads) |
| **Matrix_CSV**             | CSV export of Matrix                     | Matrix2csv (writes) |
| **LookUp_Table**           | unique_id ↔ ASIN lookup                  | Amazon_data_script (writes), Restore_LookupTable, Enrich_Lookup_Table. Scripts use **LookUp Table** (space) |
| **Matrix_Lookup**         | Row-range lookup for Matrix              | reduced_form2matrix (writes). Scripts use **MatrixLookUp** (no underscore) |
| **Meta_Data_Mat_files**   | Meta JSONL → chunked .mat                | Amazon_meta_data_script (writes). Scripts use **Meta_Data Mat_files** (space) |
| **Meta_Data_LookUp_Table**| Category ID ↔ category name             | Amazon_meta_data_script (writes), Enrich_Lookup_Table (reads). Scripts use **Meta_Data LookUp Table** |
| **Asin_Category_Matrix**  | Product × category sparse matrix         | generate_category_sparse_matrix (writes). Stored as `*_SparseMatrix.mat` in LookUp_Table (or same folder as Enriched table) |

---

## 2. Data Pipeline – Which Script Reads/Writes Where

### Stage 1: Raw reviews (JSONL → Mat_Files + LookUp_Table)

| Script                 | Reads from                    | Writes to |
|------------------------|-------------------------------|-----------|
| **Amazon_data_script.m** | User-selected **.jsonl** file | **Mat_Files** / *Category*: `{Category}{startIdx}_{endIdx}.mat` (e.g. `Magazine_Subscriptions1_10000.mat`). **LookUp_Table**: `{Category}LookUpTable.mat` |

- Input: one JSONL review file (e.g. `Magazine_Subscriptions.jsonl` or similar).
- Output: chunked `dataStruct` .mat files and one lookup table. Paths are currently hardcoded (see Section 4).

---

### Stage 2: Reduced form (Mat_Files → Processed_MAT_files)

| Script                        | Reads from              | Writes to |
|-------------------------------|-------------------------|-----------|
| **reduced_form_script_10000.m** (or helper variants) | **Mat_Files** / *Category*: `{Category}_%d_%d.mat` | **Processed_MAT_files** (script: Procceced_Mat_Files): `{Category}_processed{start}_{end}.mat`. Optional: **TooBigReviews** for oversized products |

- Uses **CleanDuplicates.m** (no paths).
- Filename pattern in script: `All_Beauty_%d_%*d.mat` → for Magazine_Subscriptions use **Magazine_Subscriptions_%d_%*d.mat** (category name must match exactly).
- Writes `combinedDataStruct` with time windows, star counts, etc.

---

### Stage 3: Matrix and row lookup (Processed_MAT_files → Matrix + Matrix_Lookup)

| Script                  | Reads from                    | Writes to |
|-------------------------|-------------------------------|-----------|
| **reduced_form2matrix.m** | **Processed_MAT_files**: `{Category}_processed_%d_%*d.mat` | **Matrix**: `{Category}_Matrix{start}_{end}.mat` (**combinedDataMatrix**). **Matrix_Lookup**: `{Category}_lookupMatrix{start}_{end}.mat` (**combinedlookupMatrix**) |

- **Filename pattern**: Script expects `Automotive_processed_%d_%*d.mat`. So processed files must be named like **Magazine_Subscriptions_processed_1_10000.mat** (with underscore before “processed”).  
- **Mismatch**: reduced_form_script_10000.m writes `All_Beauty_processed1_10000.mat` (no underscore after “processed”). For Magazine_Subscriptions you must either (a) change reduced_form_script to save `Magazine_Subscriptions_processed_1_10000.mat`, or (b) change reduced_form2matrix to parse `Magazine_Subscriptions_processed%d_%*d.mat`, so the two scripts agree.

---

### Stage 4: Matrix → CSV

| Script           | Reads from     | Writes to      |
|------------------|----------------|----------------|
| **Matrix2csv.m** | **Matrix** (user selects dir or hardcoded) | **Matrix_CSV** (hardcoded path in script) |

- Converts each Matrix .mat to a .csv with column headers.

---

### Meta and enrichment (parallel track)

| Script                        | Reads from | Writes to |
|-------------------------------|------------|-----------|
| **Amazon_meta_data_script.m** | User-selected **Meta_Data** folder with `meta_*.jsonl` | **Meta_Data_Mat_files**: `meta_{Category}_part{N}.mat`. **Meta_Data_LookUp_Table**: `meta_{Category}_Category_Lookup.mat` |
| **Restore_LookupTable.m**     | **Mat_Files** (all category subfolders) | **LookUp_Table**: `{Category}LookUpTable.mat` (rebuilt from Mat_Files only) |
| **Enrich_Lookup_Table.m**     | LookUp_Table, **Mat_Files** (processed), **Meta_Data** .mat, **Meta_Data_LookUp_Table** | **LookUp_Table**: Enriched table (parent_asin, category_ids) |
| **generate_category_sparse_matrix.m** | Enriched Lookup Table, Category Lookup Table | Same folder as Enriched table: `{EnrichedBaseName}_SparseMatrix.mat` (e.g. **Asin_Category_Matrix** / LookUp_Table) |

---

## 3. Execution Order for Magazine_Subscriptions

Run in this order:

1. **Amazon_data_script.m**  
   - Input: JSONL review file for the category.  
   - Output: **Mat_Files/Magazine_Subscriptions/** (`Magazine_Subscriptions{start}_{end}.mat`), **LookUp_Table/Magazine_SubscriptionsLookUpTable.mat**.

2. **reduced_form_script_10000.m** (or **helper/reduced_form_script_with_CleanDup.m**)  
   - Input: **Mat_Files/Magazine_Subscriptions/**.  
   - Output: **Processed_MAT_files/Magazine_Subscriptions/** (e.g. `Magazine_Subscriptions_processed_1_10000.mat` – ensure naming matches reduced_form2matrix).

3. **reduced_form2matrix.m**  
   - Input: **Processed_MAT_files/Magazine_Subscriptions/**.  
   - Output: **Matrix/** and **Matrix_Lookup/**.

4. **Matrix2csv.m**  
   - Input: **Matrix/** (or user-selected folder).  
   - Output: **Matrix_CSV/**.

**Optional / parallel:**

- **Amazon_meta_data_script.m**: run when you have `meta_*.jsonl` for the category; fills **Meta_Data_Mat_files** and **Meta_Data_LookUp_Table**.
- **Restore_LookupTable.m**: only if you need to rebuild LookUp tables from Mat_Files (e.g. no JSONL run).
- **Enrich_Lookup_Table.m**: after meta is available; enriches LookUp_Table with parent_asin and category_ids.
- **generate_category_sparse_matrix.m**: after enrichment; builds **Asin_Category_Matrix** (SparseMatrix) from Enriched table and Category Lookup.

---

## 4. Hardcoded Paths and Suggested Replacements

All paths below should be made relative to a single **category root** (e.g. `Magazine_Subscriptions` or project root) so the same scripts work for any category and machine.

### 4.1 Amazon_data_script.m

- `outputFileName = 'F:\Roy\Amazon_data\Mat_files\Home_and_Kitchen\Home_and_Kitchen'`
- `lookupTableFileName = 'F:\Roy\Amazon_data\LookUp Table\Home_and_KitchenLookUpTable.mat'`

**Suggested replacement (example with category variable):**

```matlab
categoryName = 'Magazine_Subscriptions';  % or uigetdir / input
baseDir = 'C:\Users\sheba\Desktop\Amazon-Data-\Magazine_Subscriptions';  % or pwd, fileparts(which(mfilename)), etc.
outputFileName = fullfile(baseDir, 'Mat_Files', categoryName, categoryName);  % base name for chunks
lookupTableFileName = fullfile(baseDir, 'LookUp_Table', [categoryName 'LookUpTable.mat']);
```

Ensure **Mat_Files** and **LookUp_Table** exist under `baseDir` (e.g. `mkdir` if needed).

---

### 4.2 reduced_form_script_10000.m

- `inputDir = 'F:\Amazon_data\Mat_files\All_Beauty'`
- `outputDir = 'F:\AmazonData_Part1Full\Categories\All_Beauty\Procceced_Mat_Files'`
- `largeReviewDir = 'F:\Roy\Amazon_data\TooBigReviews'`

**Suggested replacement:**

```matlab
categoryName = 'Magazine_Subscriptions';
baseDir = 'C:\Users\sheba\Desktop\Amazon-Data-\Magazine_Subscriptions';
inputDir = fullfile(baseDir, 'Mat_Files', categoryName);
outputDir = fullfile(baseDir, 'Processed_MAT_files');  % or keep typo 'Procceced_Mat_Files'
largeReviewDir = fullfile(baseDir, 'TooBigReviews');  % optional; create if used
```

Also replace the **category name in sscanf** and **sprintf** (e.g. `All_Beauty` → `Magazine_Subscriptions`) so file names match your category and match **reduced_form2matrix** (see Section 2, Stage 3).

---

### 4.3 reduced_form2matrix.m

- `inputDir = 'F:\AmazonData_Part1Full\Categories\Automotive\Procceced_Mat_Files'`
- `outputDir = 'F:\AmazonData_Part1Full\Categories\Automotive\Matrix'`
- `lookupTabledir = 'F:\AmazonData_Part1Full\Categories\Automotive\MatrixLookUp'`

**Suggested replacement:**

```matlab
categoryName = 'Magazine_Subscriptions';
baseDir = 'C:\Users\sheba\Desktop\Amazon-Data-\Magazine_Subscriptions';
inputDir = fullfile(baseDir, 'Processed_MAT_files');
outputDir = fullfile(baseDir, 'Matrix');
lookupTabledir = fullfile(baseDir, 'Matrix_Lookup');
```

Update **sscanf** to use `categoryName`:

```matlab
pattern = [categoryName '_processed_%d_%*d.mat'];  % or '_processed%d_%*d.mat' to match script_10000
fileNumbers = cellfun(@(x) sscanf(x, pattern), {matFiles.name});
```

---

### 4.4 Matrix2csv.m

- `outputDir = 'F:\AmazonData_Part1Full\Categories\Automotive\Matrix_CSV'`

**Suggested replacement:**

```matlab
% If inputDir is chosen via uigetdir, derive output from it:
outputDir = fullfile(inputDir, '..', 'Matrix_CSV');
% Or use a base directory:
baseDir = 'C:\Users\sheba\Desktop\Amazon-Data-\Magazine_Subscriptions';
outputDir = fullfile(baseDir, 'Matrix_CSV');
if ~exist(outputDir, 'dir'), mkdir(outputDir); end
```

---

### 4.5 Amazon_meta_data_script.m

- `outputFolder = 'G:\AmazonData\Meta_Data Mat_files'`
- `lookupTableDir = 'G:\AmazonData\Meta_Data LookUp Table'`

**Suggested replacement:**

```matlab
baseDir = 'C:\Users\sheba\Desktop\Amazon-Data-\Magazine_Subscriptions';
outputFolder = fullfile(baseDir, 'Meta_Data_Mat_files');
lookupTableDir = fullfile(baseDir, 'Meta_Data_LookUp_Table');
if ~exist(outputFolder, 'dir'), mkdir(outputFolder); end
if ~exist(lookupTableDir, 'dir'), mkdir(lookupTableDir); end
```

---

### 4.6 Helper scripts

- **helper/reduced_form_script_100000.m**: `F:\Roy\Amazon_data\Mat_files\Books`, `Processed_MAT_files\Books`, `TooBigReviews\Books` → same idea: `fullfile(baseDir, 'Mat_Files', categoryName)` etc.
- **helper/reduced_form_script_with_CleanDup.m**: `F:\Roy\Amazon_data\Mat_files\Clothing_Shoes_and_Jewelry`, `Processed_MAT_files\Clothing_Shoes_and_Jewelry` → use `baseDir` + category.
- **helper/Restore_LookupTable.m**: `matFilesRoot = 'G:\AmazonData\Mat_Files'`, `outputDir = 'G:\AmazonData\LookUp Table'` → e.g. `baseDir = 'C:\Users\sheba\Desktop\Amazon-Data-'`, `matFilesRoot = fullfile(baseDir, 'Magazine_Subscriptions', 'Mat_Files')`, `outputDir = fullfile(baseDir, 'Magazine_Subscriptions', 'LookUp_Table')`.
- **Enrich_Lookup_Table.m**, **ViewProductDetails.m**: use uigetfile/uigetdir; only comments/examples reference `G:\AmazonData\...`. Replace examples with your `Magazine_Subscriptions` paths.
- **Validate_Category_Mapping_Report.m**, **generate_category_sparse_matrix.m**: path-agnostic (user selects files); no hardcoded roots.

---

## 5. Missing Inputs for Magazine_Subscriptions (Checklist)

Before running the pipeline, ensure:

| # | Required for script(s) | Folder / file | Status / action |
|---|-------------------------|----------------|-----------------|
| 1 | Amazon_data_script      | **JSONL** review file (e.g. `Magazine_Subscriptions.jsonl` or category review file) | Provide and select via uigetfile. |
| 2 | Amazon_data_script      | **Mat_Files/Magazine_Subscriptions/** (created on first run) | Script will write here; folder must exist or be created. |
| 3 | Amazon_data_script      | **LookUp_Table/** | Folder for `Magazine_SubscriptionsLookUpTable.mat`; create if missing. |
| 4 | reduced_form_script     | **Mat_Files/Magazine_Subscriptions/*.mat** | Must exist after step 1. Filenames: `Magazine_Subscriptions1_10000.mat`, etc. |
| 5 | reduced_form2matrix     | **Processed_MAT_files/*.mat** | Must exist after step 2. Naming must match sscanf (e.g. `Magazine_Subscriptions_processed_1_10000.mat`). |
| 6 | reduced_form2matrix     | **Matrix/**, **Matrix_Lookup/** | Create if missing; script writes here. |
| 7 | Matrix2csv              | **Matrix/*.mat** | After step 3. |
| 8 | Matrix2csv              | **Matrix_CSV/** | Create if missing. |
| 9 | Amazon_meta_data_script | **Meta_Data** folder with `meta_*.jsonl` (e.g. `meta_Magazine_Subscriptions.jsonl`) | Optional; for enrichment. |
| 10| Enrich_Lookup_Table     | **Meta_Data_Mat_files** (e.g. `meta_Magazine_Subscriptions_part1.mat`) | After meta script. |
| 11| Enrich_Lookup_Table     | **Meta_Data_LookUp_Table** (e.g. `meta_Magazine_Subscriptions_Category_Lookup.mat`) | After meta script. |
| 12| generate_category_sparse_matrix | Enriched Lookup Table + Category Lookup Table | After Enrich_Lookup_Table. |

**Summary of likely “missing” items for a fresh Magazine_Subscriptions run:**

- **Magazine_Subscriptions** folder with subfolders: Mat_Files, Processed_MAT_files, Matrix, Matrix_CSV, LookUp_Table, Matrix_Lookup, (optional) Meta_Data_Mat_files, Meta_Data_LookUp_Table.
- **Raw input**: One JSONL review file for the category (for Amazon_data_script).
- **Optional**: Meta JSONL and meta pipeline outputs for Enrich_Lookup_Table and Asin_Category_Matrix.
- **Naming**: Align processed filenames between reduced_form_script and reduced_form2matrix (underscore before “processed” and category name in sscanf/sprintf).

---

## 6. Quick Reference – Script ↔ Folders

| Script | Reads | Writes |
|--------|--------|--------|
| Amazon_data_script | .jsonl (user) | Mat_Files, LookUp_Table |
| reduced_form_script_10000 | Mat_Files | Processed_MAT_files, (optional) TooBigReviews |
| reduced_form2matrix | Processed_MAT_files | Matrix, Matrix_Lookup |
| Matrix2csv | Matrix | Matrix_CSV |
| Amazon_meta_data_script | Meta_Data (meta_*.jsonl) | Meta_Data_Mat_files, Meta_Data_LookUp_Table |
| Restore_LookupTable | Mat_Files | LookUp_Table |
| Enrich_Lookup_Table | LookUp_Table, Mat_Files, Meta .mat, Meta_Data_LookUp_Table | LookUp_Table (enriched) |
| generate_category_sparse_matrix | Enriched Lookup, Category Lookup | *_SparseMatrix.mat (e.g. Asin_Category_Matrix) |

This document is saved as **DATA_PIPELINE_ANALYSIS.md** in the project root for future reference.
